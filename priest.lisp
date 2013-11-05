;;
;;  priest  -  HTML data extraction in Common Lisp
;;
;;  Copyright 2013,2014 Thomas de Grivel <thomas@lowh.net>
;;
;;  Permission to use, copy, modify, and distribute this software for any
;;  purpose with or without fee is hereby granted, provided that the above
;;  copyright notice and this permission notice appear in all copies.
;;
;;  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
;;  WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
;;  MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
;;  ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
;;  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
;;  ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
;;  OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
;;

(in-package :cl-user)

(defpackage :priest
  (:use :alexandria :cl :cl-debug :optima)
  (:shadow #:make-keyword)
  (:export
   #:*base-uri*
   #:*body*
   #:*charset*
   #:*content-type*
   #:*cookie-jar*
   #:*log-output*
   #:*default-fetch-retry*
   #:*doc*
   #:*download-dir*
   #:*headers*
   #:*method*
   #:*referer*
   #:*stream*
   #:*uri*
   #:*user-agent*
   #:digest
   #:digest/hex
   #:download
   #:document-fragment
   #:inner-html
   #:inner-lhtml
   #:inner-text
   #:inner-xhtml
   #:inner-xmls
   #:input-value
   #:keychain-get
   #:keychain-set
   #:request
   #:select
   #:table
   #:uri-of
   #:with-keychain
   #:with-session
   #:with-static #:with-assoc #:digest))

(in-package :priest)

(defclass session ()
  ((fetch-retry :initarg :fetch-retry
		:initform 3
		:type non-negative-fixnum
		:accessor fetch-retry)
   (user-agent :initarg :user-agent
	       :initform :firefox
	       :accessor user-agent)
   (base-uri :initarg :base-uri
	     :initform ""
	     :type string
	     :reader base-uri)
   (log-output :initarg :log-output
	       :initform (make-synonym-stream '*debug-io*)
	       :type output-stream
	       :reader log-output)
   (cookie-jar :initarg :cookie-jar
	       :initform (make-instance 'drakma:cookie-jar)
	       :type drakma:cookie-jar
	       :reader cookie-jar)
   (download-dir :initarg :download-dir
		 :initform *default-pathname-defaults*
		 :type pathname
		 :accessor download-dir)
   (referrer :initarg :referrer
	     :initform nil
	     :accessor referrer)
   (method       :accessor request-method      :initform nil)
   (stream       :accessor http-stream         :initform nil)
   (url          :accessor reply-url           :initform nil)
   (headers      :accessor reply-headers       :initform nil)
   (content-type :accessor reply-content-type  :initform nil)
   (body         :accessor reply-body          :initform nil)
   (doc          :accessor reply-doc           :initform nil)
   (charset      :accessor reply-charset       :initform nil)))

;;  Session

(defvar *session*)

(defmacro with-session (&body body)
  (let ((new-session (gensym "SESSION-"))
	(body-function (gensym "BODY-")))
    `(flet ((,body-function () ,@body))
       (if (boundp '*session*)
	   (,body-function)
	   (let ((,new-session (make-instance 'session)))
	     (unwind-protect (let ((*session* ,new-session))
			       (,body-function))
	       (when-let ((stream (http-stream ,new-session)))
		 (close stream))))))))

;;  Sugar

(defun make-keyword (str)
  (intern (string-upcase str) :keyword))

(defun find-keyword (str among)
  (find str among :test #'string-equal))

(defmacro with-assoc (bindings alist &body body)
  (let ((g!alist (gensym "ALIST-")))
    `(let ((,g!alist ,alist))
       (symbol-macrolet ,(mapcar (lambda (b)
				   (let ((var (if (consp b) (car b) b))
					 (key (if (consp b) (cdr b) (make-keyword b))))
				     `(,var (cdr (assoc ,key ,g!alist)))))
				 bindings)
	 ,@body))))

(defmacro with-static (bindings &body body)
  (let ((static (gensym "STATIC-")))
    `(let ((,static (load-time-value (vector ,@(mapcar #'cadr bindings)))))
       (symbol-macrolet ,(loop for i from 0
			       for b in bindings
			       collect `(,(car b) (svref ,static ,i)))
	 ,@body))))

(declaim (inline digest))

(defun digest (type &rest things)
  (let ((d (ironclad:make-digest type)))
    (labels ((digest<- (x)
	       (declare (inline digest<-))
	       (etypecase x
		 (null)
		 ((array (unsigned-byte 8) (*)) (ironclad:update-digest d x))
		 (string (digest<- (trivial-utf-8:string-to-utf-8-bytes x)))
		 (character (digest<- (make-string 1 :initial-element x)))
		 (symbol (digest<- (symbol-name x)))
		 (puri:uri (digest<- (puri:render-uri x nil))))))
      (declare (inline digest<-))
      (mapc #'digest<- things))
    (force-output *log-output*)
    (ironclad:produce-digest d)))

(defun digest/hex (type &rest things)
  (ironclad:byte-array-to-hex-string (apply #'digest type things)))

;;  URI

(defun trim-spaces (s)
  (string-trim '(#\Newline #\Return #\Space #\Tab) s))

(defun uri-filename (uri)
  (cl-ppcre:scan-to-strings "[^/]+$" (puri:uri-path uri)))

(defun puri-of (thing &optional (base *base-uri*))
  (puri:merge-uris
   (puri:uri
    (or (when (dom:element-p thing)
	  (case (make-keyword (dom:tag-name thing))
	    ((:a :link) (trim-spaces (dom:get-attribute thing "href")))
	    ((:option) (trim-spaces (dom:get-attribute thing "value")))))
	thing))
   base))

(defun uri-of (thing &optional (base *base-uri*))
  (puri:render-uri (puri-of thing base) nil))

;;  Parse content

(defgeneric parse-content (body content-type))

(defmethod parse-content (body content-type)
  (when (debug-p :priest)
    (format *log-output* "~&Don't know how to parse ~S with Content-Type: ~A~%"
	    (type-of body) content-type))
  body)

(defmethod parse-content (body (content-type (eql t)))
  body)

(defmethod parse-content (body (content-type (eql :text/plain)))
  body)

(defmethod parse-content (body (content-type (eql :text/html)))
  (closure-html:parse body (cxml-dom:make-dom-builder)))

(defmethod parse-content ((body string) (content-type (eql :application/json)))
  (cl-json:decode-json-from-string body))

(defmethod parse-content ((body string) (content-type (eql :application/javascript)))
  body)

(defmethod parse-content ((body array) (content-type (eql :application/javascript)))
  (parse-content (the string (trivial-utf-8:utf-8-bytes-to-string body)) content-type))

(defmethod parse-content ((body string) (content-type (eql :text/css)))
  body)

(defmethod parse-content ((body array) (content-type (eql :text/css)))
  (parse-content (the string (trivial-utf-8:utf-8-bytes-to-string body)) content-type))


;;  HTTP

(defmacro do-header-parameters ((var header) &body body)
  `(cl-ppcre:do-register-groups (,var) (";\\s*([^;]+)\\s*" ,header)
     ,@body))

(defun parse-content-type-header (headers)
  (let* ((header (cdr (assoc :content-type headers)))
	 (type (make-keyword (cl-ppcre:scan-to-strings "[-\\w]+/[-\\w]+" header)))
	 params)
      (do-header-parameters (param header)
	(push (or (cl-ppcre:register-groups-bind (name val) ("(\\S+)=(\\S+)" param)
		    (cond ((string-equal name :charset)
			   (cons :charset (make-keyword val)))
			  (:otherwise (cons name val))))
		  param)
	      params))
      (cons type params)))

(defun fix-content-type (type)
  (case type
    (:application/x-javascript :application/javascript)
    (otherwise type)))

(defun parse-content-disposition-header (headers)
  (let* ((header (cdr (assoc :content-disposition headers)))
	 (type (make-keyword (cl-ppcre:scan-to-strings "[-\\w]+" header)))
	 params)
      (do-header-parameters (param header)
	(push (or (cl-ppcre:register-groups-bind (name quoted unquoted)
		      ("(\\S+)=(?:\"([^\"]*)\"|(\\S+))" param)
		    (let ((val (or quoted unquoted)))
		      (cond ((string-equal name :filename)
			     (cons :filename val))
			    (:otherwise (cons name val)))))
		  param)
	      params))
      (cons type params)))

(defmacro with-refetch-restart ((retry fmt &rest args) &body body)
  (let ((refetch (gensym "REFETCH-"))
	(try (gensym "TRY-"))
	(result (gensym "RESULT-")))
    `(let ((,try 0)
	   (,result))
       (tagbody
	,refetch
	  (incf ,try)
	  (restart-bind ((refetch (lambda () (go ,refetch))
			   :report-function (lambda (s) (format s ,fmt ,@args))))
	    (handler-bind ((error (lambda (c)
				    (format *log-output* "~&~A (try ~D)~%" c ,try)
				    (when (<= ,try ,retry)
				      (invoke-restart 'refetch)))))
	      (setf ,result (locally ,@body)))))
       ,result)))

(defun request (method thing &key additional-headers content content-type
			       (expect-status (if (eq :post method)
						  '(200 302)
						  '(200)))
			       override-content-type
			       parameters
			       (referer (or *referer* (when (boundp '*uri*) *uri*)))
			       (retry *default-fetch-retry*))
  (when referer (push `(:referer . ,referer) additional-headers))
  (let ((uri (puri-of thing)))
    (with-refetch-restart (retry "Retry request ~A ~S" method (puri:render-uri uri nil))
      (format *log-output* "~&~4A ~A~%" method uri)
      (force-output *log-output*)
      (multiple-value-bind (body status headers uri stream close reason)
	  (handler-bind ((flexi-streams:external-format-encoding-error
			  (lambda (e)
			    (warn "~A" e)
			    (invoke-restart 'use-value (code-char #xFFFD)))))
	    (drakma:http-request uri
				 :additional-headers additional-headers
				 :close nil
				 :content content
				 :content-type content-type
				 :cookie-jar *cookie-jar*
				 :keep-alive t
				 :method method
				 :parameters parameters
				 :stream (when (and *stream* (open-stream-p *stream*))
					   *stream*)
				 :user-agent (or *user-agent* :firefox)))
	(setf *stream* (if close nil stream))
	(let* ((content-type-header (parse-content-type-header headers))
	       (content-type (or override-content-type (fix-content-type
							(car content-type-header))))
	       (charset (cdr (assoc :charset (cdr content-type-header)))))
	  (when (and (eq :utf8 charset)
		     (typep body '(array (unsigned-byte 8))))
	    (setf body (trivial-utf-8:utf-8-bytes-to-string body)))
	  (assert (find status expect-status) nil
		  " ~A ~A~% ~S~% ~D ~A~% Expected ~D~% BODY ~S~% HEADERS ~S"
		  method uri additional-headers status reason expect-status
		  body headers)
	  (setf *uri* uri
		*headers* headers
		*content-type* content-type
		*charset* charset
		*body* body
		*doc* (parse-content *body* content-type)))))))

(defgeneric empty-p (x))

(defmethod empty-p ((x sequence))
  (= 0 (length x)))

(defmethod empty-p ((x symbol))
  (null x))

(defun download (thing &key additional-headers
			 (into *download-dir*)
			 (method :get)
			 parameters
			 (referer (or *referer* *uri*))
			 replace
			 (retry *default-fetch-retry*))
  (when referer (push `(:referer . ,referer) additional-headers))
  (with-simple-restart (ignore "Ignore download")
    (let ((uri (puri-of thing)))
      (with-refetch-restart (retry "Retry downloading ~S" (puri:render-uri uri nil))
	(format *log-output* "~&DL   ~A~%" uri)
	(force-output *log-output*)
	(multiple-value-bind (in status headers uri stream close reason)
	    (drakma:http-request uri
				 :additional-headers additional-headers
				 :method method
				 :parameters parameters
				 :cookie-jar *cookie-jar*
				 :user-agent (or *user-agent* :firefox)
				 :stream (and *stream* (open-stream-p *stream*)
					      *stream*)
				 :want-stream t)
	  (declare (ignorable headers stream close))
	  (setf *stream* nil)
	  (assert (= 200 status) nil "~D ~A ~A" status reason uri)
	  (let* ((content-disposition-header (parse-content-disposition-header headers))
		 (filename (or (cdr (assoc :filename (cdr content-disposition-header)))
			       (car (last (remove-if #'empty-p (puri:uri-parsed-path
								uri))))))
		 (path (namestring
			(if (pathname-name into)
			    into
			    (merge-pathnames (or filename (uri-filename uri))
					     (merge-pathnames into *download-dir*)))))
		 (path.part (format nil "~A.part" path))
		 (in-stream (flexi-streams:flexi-stream-stream in)))
	    (when (and (probe-file path) (not replace))
	      (format *log-output* "~&Not replacing ~S~%" path)
	      (return-from download nil))
	    (format *log-output* "~&Into ~S~%" path)
	    (ensure-directories-exist path)
	    (force-output *log-output*)
	    (unwind-protect (progn (with-open-file
				       (out path.part :direction :output
					    :element-type (stream-element-type
							   in-stream))
				     (alexandria:copy-stream in-stream out))
				   (rename-file path.part path)
				   (setf path.part nil))
	      (when path.part (delete-file path.part)))
	    path))))))

;;  Selectors

(defun select (css-selector &optional (document *doc*))
  (css-selectors:query css-selector document))

;;  Document fragments

(defgeneric document-fragment (thing))

(defmethod document-fragment ((node dom:node))
  (let* ((doc (dom:owner-document node))
	 (frag (dom:create-document-fragment doc)))
    (dom:append-child frag (dom:clone-node node t))
    frag))

(defmethod document-fragment ((nodes sequence))
  (let* ((doc (dom:owner-document (elt nodes 0)))
	 (frag (dom:create-document-fragment doc)))
    (map nil (lambda (node)
	       (dom:append-child frag (dom:clone-node node t)))
	 nodes)
    frag))

;;  Inner HTML

(defgeneric inner-html (x))
(defgeneric inner-xhtml (x))
(defgeneric inner-lhtml (x))

(defmethod inner-html ((node dom:node))
  (dom:map-document (chtml:make-string-sink)
		    (document-fragment (dom:child-nodes node))))

(defmethod inner-html ((nodes sequence))
  (dom:map-document (chtml:make-string-sink)
		    (document-fragment nodes)))

(defmethod inner-xhtml ((node dom:node))
  (dom:map-document (cxml:make-string-sink :omit-xml-declaration-p t)
		    (document-fragment (dom:child-nodes node))))

(defmethod inner-xmls ((node dom:node))
  (cddr (dom:map-document (cxml-xmls:make-xmls-builder
			   :include-namespace-uri nil)
			  (document-fragment node))))

(defmethod inner-lhtml ((node dom:node))
  (cddr (dom:map-document (chtml:make-lhtml-builder)
			  (document-fragment node))))

;;  Inner text

(defclass text-sink (hax:abstract-handler)
  ((stream :type stream :accessor text-sink-stream)
   (links :accessor text-sink-links :initform nil)))

(defun make-text-sink ()
  (make-instance 'text-sink))

(defmethod sax:start-document ((handler text-sink))
  (setf (text-sink-stream handler) (make-string-output-stream)))

(defvar *block-elements*
  '(br div h1 h2 h3 h4 h5 h6 hr input pre textarea table tr))

(defvar *tags*
  `(a ,.*block-elements*))

(defun tag (x)
  (find x *tags* :test #'string-equal))

(defmethod sax:start-element ((handler text-sink) ns lname qname attr)
  (let ((tag (tag lname)))
    (cond #+nil((eq 'a tag) (let ((href (sax:find-attribute "href" attr)))
			 (when href
			   (setq href (puri:uri (string-trim '(#\Newline #\Return #\Space #\Tab) (sax:attribute-value href)))))
			 (push href (text-sink-links handler))))
	  ((find tag *block-elements*)
	   (fresh-line (text-sink-stream handler))))))

(defmethod sax:end-element ((handler text-sink) ns lname qname)
  (let ((tag (tag lname)))
    (cond #+nil((eq 'a tag) (let ((href (pop (text-sink-links handler))))
			 (when href
			   (format (text-sink-stream handler)
				   "~&~A)" href))))
	  ((find tag *block-elements*)
	   (fresh-line (text-sink-stream handler))))))

(defmethod sax:characters ((handler text-sink) data)
  (write-string (cl-ppcre:regex-replace "\\s+" data " ")
		(text-sink-stream handler)))

(defmethod sax:end-document ((handler text-sink))
  (let ((stream (text-sink-stream handler)))
    (close stream)
    (get-output-stream-string stream)))

(defgeneric inner-text (node))

(defmethod inner-text ((node dom:node))
  (dom:map-document (make-text-sink)
		    (document-fragment node)))

;;  Form value

(defgeneric input-value (thing))

(defmethod input-value ((node dom:node))
  (dom:get-attribute node "value"))

(defmethod input-value ((name string))
  (let ((input (first
		(select
		 (with-output-to-string (s)
		   (loop for tag in '(input select textarea submit button)
		      for comma = "" then ","
		      do (format s "~A~A[name=~S]"
				 comma (string-downcase tag) name)))))))
    (when input
      (input-value (the dom:node input)))))

;;  Table

(defgeneric table (thing))

(defmethod table ((elements sequence))
  (map 'list #'table elements))

(defmethod table ((element dom:element))
  (let ((tag (find (dom:tag-name element)
		   '(table thead tbody tr)
		   :test #'string-equal)))
    (format t "TAG ~S~%" tag)
    (case tag
      ((table thead tbody) (table (dom:child-nodes element)))
      ((tr) (map 'list #'inner-text (dom:child-nodes element))))))

;;  Keychain

(defvar *keychain* (make-hash-table :test 'equal))

(defun keychain-get (property &optional (uri *uri*))
  (let ((u (puri-of uri)))
    (flet ((try-uri ()
	     (let ((r (getf (gethash (puri:render-uri u nil) *keychain*)
			    property)))
	       (when r
		 (return-from keychain-get r)))))
      (declare (inline try-uri))
      (try-uri)
      (setf (puri:uri-fragment u) nil)
      (try-uri)
      (setf (puri:uri-query u) nil)
      (try-uri)
      (setf (puri:uri-path u) nil)
      (try-uri)
      ; FIXME: try each upper directories of path
      ; FIXME: try removing subdomains
      (error "Keychain property ~S not found for ~S" property uri))))

(defun keychain-set (uri &rest plist)
  (setf (gethash uri *keychain*) plist)
  uri)

(defmacro with-keychain (properties &body body)
  (destructuring-bind (&key uri) (when (keywordp (caar body)) (pop body))
    `(let ,(mapcar (lambda (p)
		     `(,p (keychain-get ,(make-keyword (symbol-name p))
					,@(when uri `(,uri)))))
		   properties)
       ,@body)))
