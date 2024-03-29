#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/lattice)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
;; This module needs to go early because it (temporarily) disables the brico database
(use-module 'brico/build/index)

(use-module '{knodb knodb/search knodb/fuzz knodb/branches})
(use-module '{brico brico/indexing})

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (index-lattice/prefetch (qc oids))))

(defambda (index-batch frames batch-state loop-state task-state)
  (let* ((index (try (get batch-state 'index) (get loop-state 'index))))
    (prefetch-oids! frames)
    (do-choices (f frames)
      (knodb/index*! index f genls* genls specls*)
      (knodb/index*! index f partof* partof parts*)
      (knodb/index*! index f memberof* memberof members*)
      (knodb/index*! index f ingredientof* ingredientof ingredients*))
    (swapout frames)))

(define (main poolname)
  (config! 'appid (glom "index-" (basename poolname ".pool") "-lattice"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search 
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text}))
  (let* ((pool (getdbpool poolname))
	 (target (pool/index/target pool 'name 'lattice))
	 (frames (pool-elts pool)))
    (engine/run index-batch (difference frames (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[index ,target]
	 batchsize ,(config 'batchsize 10000)
	 nthreads ,(config 'nthreads #t)
	 branchindexes index
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pool target}
	 logfns {,engine/log ,engine/logrusage}
	 logfreq ,(config 'logfreq 50)
	 logchecks #t])
    (commit)))
(module-export! 'main)

(when (config 'optimize #t config:boolean)
  (optimize! '{knodb knodb/branches knodb/search 
	       knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
	       knodb/tinygis
	       fifo engine})
  (optimize! '{brico brico/indexing})
  (optimize-locals!))
