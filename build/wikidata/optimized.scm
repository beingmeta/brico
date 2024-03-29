;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase  (ken.haase@alum.mit.edu)

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

