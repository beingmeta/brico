(use-module '{logger optimize engine})
(use-module '{brico brico/wikid})
(use-module '{brico/build/wikidata brico/build/wikidmap})
(load-config "local.cfg")

(dbctl {brico.pool wikid.pool 
	(dbctl brico.index 'partitions)
	(dbctl wikid.index 'partitions)}
       'readonly #f)

(defambda (update-wikidata batch batch-state loop-state task-state)
  (prefetch-oids! batch)
  (prefetch-oids! (pickoids (get buildmap.table (get batch 'wikidref))))
  (do-choices (f batch)
    (wikid/copy! (get buildmap.table (get f 'wikidref)) f
		 #f #f)))

(define (update-all)
  (engine/run update-wikidata (?? 'has 'wikidref) 
    `#[checkpoint ,{brico.pool wikid.pool (dbctl {brico.pool wikid.pool} 'partitions)}
       nthreads 16
       batchsize ,(config 'BATCHSIZE 5000)
       checkfreq 15
       checktests ,(engine/delta 'items 50000)]))

(define main update-all)

(when (config 'optimized #t)
  (optimize! '{brico brico/wikid brico/indexing
	       brico/build/wikidata brico/build/wikidmap
	       engine})
  (optimize!))

(config! 'wikidata:build #t)

