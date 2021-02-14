;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata/optimized)

(use-module '{optimize})

(use-module '{brico brico/indexing
	      brico/build/wikidata
	      brico/build/wikidata/map
	      brico/build/wikidata/init
	      brico/build/wikidata/read
	      brico/build/wikidata/automaps})

(optimize-module!
 '{brico brico/indexing
   brico/build/wikidata
   brico/build/wikidata/map
   brico/build/wikidata/init
   brico/build/wikidata/read
   brico/build/wikidata/automaps})

