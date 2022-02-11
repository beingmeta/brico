#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/general)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
(use-module '{knodb knodb/branches knodb/search knodb/fuzz})
(use-module 'brico/build/index)
(use-module '{brico brico/indexing})

(config! 'indexinfer #f)

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define graph.index (target-file "graph.index"))
(define props.index (target-file "props.index"))
(define misc-slotids '{PERTAINYM REGION COUNTRY})

(define done #f)
(define (bgcommit)
  (until done (sleep 15) (commit)))

(define skip-slotids
  {termlogic-slotids lattice-slotids
   (?? 'type 'lexslot)})

(define (index-node f props.index graph.index (opts #f))
  (let* ((keys (getkeys f))
	 (slotids {(intersection keys misc-slotids)
		   (difference (pickoids keys) skip-slotids)}))
    (do-choices (slotid slotids)
      (knodb/index+! props.index f slotid #default opts #default graph.index))))

(defambda (general-indexer frames batch-state loop-state task-state)
  (let* ((index (get loop-state 'index))
	 (props.index (index/branch (get loop-state 'props.index)))
	 (graph.index (index/branch (get loop-state 'graph.index)))
	 (opts (try (get loop-state 'indexopts) #f)))
    (prefetch-oids! frames)
    (do-choices (f frames) (index-node f props.index graph.index opts))
    (branch/merge! props.index)
    (branch/merge! graph.index)
    (swapout frames)))

(define (main poolname)
  (config! 'appid (glom "index-" (basename poolname ".pool") "-general"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search 
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text knodb/fuzz/graph}))
  (let* ((pool (getdbpool poolname))
	 (props.index (target-index props.index #f pool 20.0))
	 (graph.index (target-index graph.index #f pool 10.0)))
    (commit pool) ;; Save metadata
    (engine/run general-indexer 
	(difference (pool-elts pool) (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[props.index ,props.index graph.index ,graph.index]
	 batchsize 2000 batchrange 3
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pool graph.index props.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])
    (commit)))
(module-export! 'main)

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize-locals!))
