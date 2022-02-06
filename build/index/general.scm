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
(define props.index (target-file "props.index"))latt
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

(define (main . names)
  (config! 'appid (glom "index-" (basename (car names) ".pool") "-general"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search 
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text knodb/fuzz/graph}))
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (props.index (target-index props.index #f pools))
	 (graph.index (target-index graph.index #f pools)))
    (do-choices (pool pools)
      (dbctl pool 'metadata 'indexes
	     (choice (dbctl pool 'metadata 'indexes) "props.index" "graph.index")))
    (commit pools) ;; Save metadata
    (engine/run general-indexer 
	(difference (pool-elts pools) (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[props.index ,props.index graph.index ,graph.index]
	 batchsize 2000 batchrange 3
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools graph.index props.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])
    (commit)))
(module-export! 'main)

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize-locals!))
