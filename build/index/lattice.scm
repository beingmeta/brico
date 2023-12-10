#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/lattice)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
;; This module needs to go early because it (temporarily) disables the brico database
(use-module '{knodb knodb/search knodb/fuzz knodb/branches})
(use-module '{brico brico/indexing brico/build/index})

(define %loglevel %notice%)

(define %optmods
  '{knodb knodb/branches knodb/search
    knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
    knodb/tinygis
    fifo engine
    brico brico/indexing})

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

(define (main poolname (output #f))
  (config! 'appid (glom "index-" (basename poolname ".pool") "-lattice"))
  (let* ((pool (knodb/ref poolname (and output [altroot output])))
	 (target (pool/index/target pool output 'name 'lattice))
	 (frames (pool-elts pool)))
    (%watch "indexlattice" pool target)
    (engine/run index-batch
        (difference frames (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[index ,target]
	 batchsize ,(config 'batchsize 10000)
	 nthreads ,(config 'nthreads #t)
	 branchindexes index
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pool target}
	 logfns {,engine/log ,engine/logrusage}
	 logfreq ,(config 'logfreq 20)
	 logchecks #t])
    (commit)))
(module-export! 'main)

(config! 'tracepoints knodb/index*!)
