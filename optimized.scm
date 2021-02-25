;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'brico/optimized)

(use-module 'optimize)
(use-module '{brico brico/wikid
	      brico/indexing brico/lookup brico/analytics
	      brico/maprules brico/dterms brico/dtermcache
	      brico/html})

(optimize! '{brico brico/wikid
	     brico/indexing brico/lookup brico/analytics
	     brico/maprules brico/dterms brico/dtermcache
	     brico/html})
