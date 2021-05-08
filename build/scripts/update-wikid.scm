(use-module '{logger optimize engine})
(use-module '{brico brico/wikid})
(use-module '{brico/build/wikidata brico/build/wikidata/map})
(load-config "local.cfg")

(config! 'brico:readonly #f)
(config! 'wikid:readonly #f)

(defambda (update-wikidata batch batch-state loop-state task-state)
  (prefetch-oids! batch)
  (prefetch-oids! (pickoids (get-wikidref (get batch 'wikidref))))
  (do-choices (f batch)
    (wikid/copy! (get-wikidref (get f 'wikidref)) f
		 #f #f)))

(defambda (update-concepts (concepts) (opts #f))
  (default! concepts (?? 'has 'wikidref))
  (engine/run update-wikidata concepts 
    `#[checkpoint ,{brico.pool wikid.pool (dbctl {brico.pool wikid.pool} 'partitions)}
       nthreads ,(getopt opts 'nthreads (config 'nthreads #t))
       batchsize ,(config 'BATCHSIZE 5000)
       checkfreq 15
       checktests ,(engine/delta 'items (getopt opts 'checkbatch 50000))]))

(define main update-concepts)

(when (config 'optimized #t)
  (optimize! '{brico brico/wikid brico/indexing
	       brico/build/wikidata brico/build/wikidata/map
	       engine})
  (use-module 'brico/optimized)
  (optimize-locals!))

