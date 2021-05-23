;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata/actions/update)

(when (file-exists? "local.cfg") (load-config "local.cfg"))

(define %optmods
  '{brico brico/wikid brico/indexing
    brico/build/wikidata brico/build/wikidata/map
    brico/build/wikidata/automap
    brico/build/wikidata/actions/update
    engine})

(use-module '{logger logctl optimize engine})
(use-module '{knodb})
(use-module '{brico brico/wikid})
(use-module '{brico/build/wikidata brico/build/wikidata/map})

(module-export! '{main update-concepts})

(when (file-exists? "local.cfg") (load-config "local.cfg"))

(config! 'brico:readonly #f)
(config! 'wikid:readonly #f)

(defambda (update-enginefn batch batch-state loop-state task-state)
  (prefetch-oids! batch)
  (prefetch-oids! (pickoids (get-wikidref (get batch 'wikidref))))
  (do-choices (f batch)
    (wikid/copy! (get-wikidref (get f 'wikidref)) f
		 #f #f)))

(define (setup)
  (knodb/readonly! brico.pool #f)
  (knodb/readonly! brico.index #f)
  (knodb/readonly! wikid.pool #f)
  (knodb/readonly! wikid.index #f))

(defambda (update-concepts (concepts) (opts #f))
  (default! concepts (?? 'has 'wikidref))
  (engine/run update-enginefn concepts 
    `#[checkpoint ,{brico.pool wikid.pool (dbctl {brico.pool wikid.pool} 'partitions)}
       nthreads ,(getopt opts 'nthreads (config 'nthreads #t))
       batchsize ,(config 'BATCHSIZE 5000)
       checkfreq 15
       checktests ,(engine/delta 'items (getopt opts 'checkbatch 50000))]))

(define (main)
  (setup)
  (update-concepts))



