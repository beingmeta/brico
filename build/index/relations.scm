#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/relations)

(use-module '{texttools varconfig logger optimize text/stringfmts knodb knodb/branches engine})
(use-module 'brico/build/index)
(use-module '{brico brico/indexing})

(config! 'indexinfer #f)

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define relations.index (target-file "relations.index"))
(define fields.index (target-file "fields.index"))
(define misc-relations '{PERTAINYM REGION COUNTRY})

(define done #f)
(define (bgcommit)
  (until done (sleep 15) (commit)))

(define skip-slotids
  {termlogic-slotids lattice-slotids
   (?? 'type 'lexslot)
   (?? 'type 'wikidprop)))

(define (index-node f index)
  (let* ((slotids (difference (pickoids (getkeys f)) skip-slotids))
	 ))
  (index-relations index f)
  (index-refterms index f)
  (do-choices (slotid misc-relations)
    (index-frame index f slotid (pickoids (get f slotid)))))

(defambda (relations-indexer frames batch-state loop-state task-state)
  (let* ((index (get loop-state 'index))
	 (branch (index/branch index)))
    (prefetch-oids! frames)
    (do-choices (f frames) (index-node f branch))
    (branch/merge! branch)
    (swapout frames)))

(define (main . names)
  (config! 'appid "index-relations")
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (fields-index (target-index fields.index #f pools))
	 (relns-index (target-index relations.index #f pools))
	 (isa-index (target-index "isa.index" #f pools #default isa))
	 (index  (make-aggregate-index {relns-index fields-index isa-index}
				       [register #t])))
    (engine/run relations-indexer (difference (pool-elts pools) (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[index ,index]
	 batchsize 2000 batchrange 3
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])
    (commit)))
(module-export! 'main)

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize-locals!))
