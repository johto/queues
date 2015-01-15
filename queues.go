package main

import (
	"code.google.com/p/plotinum/plot"
	"code.google.com/p/plotinum/plotter"
	"code.google.com/p/plotinum/plotutil"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var numRaces int64

type initFunctionType func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error)

// The baby's first implementation
func naiveUpdate() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM items
				WHERE grabbed IS NULL
				ORDER BY itemid
				LIMIT 1
				FOR UPDATE
			)
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int
			err := grabstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// The baby's first implementation, but avoid having to update every row twice
func naiveUpdateSingleTxn() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabandmarkstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = (
				SELECT itemid
				FROM items
				WHERE processed IS NULL
				ORDER BY itemid
				LIMIT 1
				FOR UPDATE
			)
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int

			// XXX DO NOT DO THIS IN A REAL APP XXX
			_, err = conn.Exec(`BEGIN`)
			if err != nil {
				panic(err)
			}

			err := grabandmarkstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				_, _ = conn.Exec(`ROLLBACK`)
				return -1
			} else if err != nil {
				panic(err)
			}

			_, err = conn.Exec(`COMMIT`)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// Skip locked rows using advisory locks when grabbing an item; random offset
// to avoid races.
func advisoryLocksRandomOffset(numConnections int) initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM
				(
					SELECT itemid
					FROM items
					WHERE grabbed IS NULL
					ORDER BY itemid
					-- maximum supported concurrency
					OFFSET $1
					LIMIT 32
				) potential_items
				WHERE pg_try_advisory_xact_lock(itemid)
				ORDER BY itemid
				LIMIT 1
			) AND
			grabbed IS NULL -- guard against the race
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE grabbed IS NULL ORDER BY itemid)
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int

		again:
			err := grabstmt.QueryRow(rand.Intn(numConnections*2)).Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					atomic.AddInt64(&numRaces, 1)
					goto again
				}
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// Skip locked rows using advisory locks when grabbing an item; naive version
// with transaction-scoped locking.
func advisoryLocksNaive() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM
				(
					SELECT itemid
					FROM items
					WHERE grabbed IS NULL
					ORDER BY itemid
					-- maximum supported concurrency
					LIMIT 32
				) potential_items
				WHERE pg_try_advisory_xact_lock(itemid)
				ORDER BY itemid
				LIMIT 1
			) AND
			grabbed IS NULL -- guard against the race
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE grabbed IS NULL ORDER BY itemid)
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int

		again:
			err := grabstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					atomic.AddInt64(&numRaces, 1)
					goto again
				}
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// approach used by https://github.com/chanks/que/blob/master/lib/que/sql.rb
func advisoryLocksQue() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			SELECT itemid
			FROM
			(
				SELECT itemid
				FROM items
				WHERE processed IS NULL
				ORDER BY itemid
				-- max concurrency
				LIMIT 32
			) items
			WHERE pg_try_advisory_lock(itemid)
			LIMIT 1
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT processed IS NULL FROM items WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}
		releaselockstmt, err := conn.Prepare(`
			SELECT pg_advisory_unlock($1)
		`)
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int

		again:
			err := grabstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				return -1
			}
			var stillAvailable bool
			err = racecheckstmt.QueryRow(itemid).Scan(&stillAvailable)
			if err != nil {
				panic(err)
			}
			if (!stillAvailable) {
				atomic.AddInt64(&numRaces, 1)
				_, err = releaselockstmt.Exec(itemid)
				if err != nil {
					panic(err)
				}
				goto again
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			_, err = releaselockstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}

}

// Skip locked rows using advisory locks when grabbing an item, but hold the
// lock for as long as possible without maintaining state.
func advisoryLocksHold() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM
				(
					SELECT itemid
					FROM items
					WHERE grabbed IS NULL
					ORDER BY itemid
					-- maximum supported concurrency
					LIMIT 32
				) potential_items
				WHERE pg_try_advisory_lock(itemid)
				ORDER BY itemid
				LIMIT 1
			) AND
			grabbed IS NULL -- guard against the race
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE grabbed IS NULL ORDER BY itemid),
				   pg_advisory_unlock_all()
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
			RETURNING pg_advisory_unlock($1)
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int

		again:
			err := grabstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				var notUsed string
				err = racecheckstmt.QueryRow().Scan(&notDone, &notUsed)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					atomic.AddInt64(&numRaces, 1)
					goto again
				}
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}


// Skip locked rows using advisory locks when grabbing an item, but hold the
// lock for a while longer to try and minimize the number of races.  Batch
// release version.
func advisoryLocksBatchRelease() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM
				(
					SELECT itemid
					FROM items
					WHERE grabbed IS NULL
					ORDER BY itemid
					-- maximum supported concurrency
					LIMIT 32
				) potential_items
				WHERE pg_try_advisory_lock(itemid)
				ORDER BY itemid
				LIMIT 1
			) AND
			grabbed IS NULL -- guard against the race
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE grabbed IS NULL ORDER BY itemid)
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}
		releaselocksstmt, err := conn.Prepare(`
			SELECT pg_advisory_unlock_all()
		`)

		numLocksHeld := 0
		return func(conn *sql.Conn) int {
			var itemid int

		again:
			if numLocksHeld >= 16 {
				_, err = releaselocksstmt.Exec()
				if err != nil {
					panic(err)
				}
			}
			numLocksHeld++

			err := grabstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					atomic.AddInt64(&numRaces, 1)
					goto again
				}
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// Skip locked rows using advisory locks when grabbing an item, but hold the
// lock for a while longer to try and minimize the number of races.
// Fixed-length queue version.
func advisoryLocksFixedLengthQueue() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM
				(
					SELECT itemid
					FROM items
					WHERE grabbed IS NULL
					ORDER BY itemid
					-- maximum supported concurrency
					LIMIT 32
				) potential_items
				WHERE pg_try_advisory_lock(itemid)
				ORDER BY itemid
				LIMIT 1
			) AND
			grabbed IS NULL -- guard against the race
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE grabbed IS NULL ORDER BY itemid),
				   pg_advisory_unlock_all()
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
			RETURNING pg_advisory_unlock($2)
		`)
		if err != nil {
			return nil, err
		}

		itemQueue := make([]int, 16)
		var itemQueuePos int
		clearQueue := func() {
			for i := range itemQueue {
				itemQueue[i] = -1
			}
			itemQueuePos = 0
		}
		clearQueue()

		return func(conn *sql.Conn) int {
			var itemid int

		again:
			err := grabstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				var notUsed string
				err = racecheckstmt.QueryRow().Scan(&notDone, &notUsed)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					clearQueue()
					atomic.AddInt64(&numRaces, 1)
					goto again
				}
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid, itemQueue[itemQueuePos])
			if err != nil {
				panic(err)
			}
			itemQueue[itemQueuePos] = itemid
			itemQueuePos = (itemQueuePos + 1) % 16
			return itemid
		}, err
	}
}

// Skip locked rows using advisory locks when grabbing an item (the "hold"
// version); single txn.
func advisoryLocksSingleTxn() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabandmarkstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = (
				SELECT itemid
				FROM
				(
					SELECT itemid
					FROM items
					WHERE processed IS NULL
					ORDER BY itemid
					-- maximum supported concurrency
					LIMIT 32
				) potential_items
				WHERE pg_try_advisory_lock(itemid)
				ORDER BY itemid
				LIMIT 1
			) AND
			processed IS NULL -- guard against the race
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE processed IS NULL ORDER BY itemid)
		`)
		if err != nil {
			return nil, err
		}
		releaselocksstmt, err := conn.Prepare(`
			SELECT pg_advisory_unlock_all()
		`)

		processedItemsSinceRelease := 0
		return func(conn *sql.Conn) int {
			var itemid int

		again:
			if processedItemsSinceRelease >= 10 {
				_, err = releaselocksstmt.Exec()
				if err != nil {
					panic(err)
				}
				processedItemsSinceRelease = 0
			}
			processedItemsSinceRelease++

			// XXX DO NOT DO THIS IN A REAL APP XXX
			_, err = conn.Exec(`BEGIN`)
			if err != nil {
				panic(err)
			}

			err := grabandmarkstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					atomic.AddInt64(&numRaces, 1)
					goto again
				}

				_, _ = conn.Exec(`ROLLBACK`)
				return -1
			} else if err != nil {
				panic(err)
			}

			_, err = conn.Exec(`COMMIT`)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// Naive update, but skip random rows
func randomOffset(numConnections int) initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM items
				WHERE grabbed IS NULL
				ORDER BY itemid
				OFFSET $1
				LIMIT 1
				FOR UPDATE
			)
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		// we could do better than this since we know the queue isn't getting
		// filled, but that would be cheating
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE grabbed IS NULL ORDER BY itemid)
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int

		again:
			err := grabstmt.QueryRow(rand.Intn(numConnections*2)).Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					atomic.AddInt64(&numRaces, 1)
					goto again
				}
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// Naive update, skip random rows, single txn
func randomOffsetSingleTxn(numConnections int) initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabandmarkstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = (
				SELECT itemid
				FROM items
				WHERE processed IS NULL
				ORDER BY itemid
				OFFSET $1
				LIMIT 1
				FOR UPDATE
			)
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		racecheckstmt, err := conn.Prepare(`
			SELECT EXISTS (SELECT 1 FROM items WHERE processed IS NULL ORDER BY itemid)
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int

			// XXX DO NOT DO THIS IN A REAL APP XXX
			_, err = conn.Exec(`BEGIN`)
			if err != nil {
				panic(err)
			}

		again:
			err := grabandmarkstmt.QueryRow(rand.Intn(numConnections*2)).Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
					atomic.AddInt64(&numRaces, 1)
					goto again
				}

				_, _ = conn.Exec(`ROLLBACK`)
				return -1
			} else if err != nil {
				panic(err)
			}

			_, err = conn.Exec(`COMMIT`)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// SKIP LOCKED, two transactions
func skipLocked() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabstmt, err := conn.Prepare(`
			UPDATE items
				SET grabbed = now()
			WHERE itemid = (
				SELECT itemid
				FROM items
				WHERE grabbed IS NULL
				ORDER BY itemid
				LIMIT 1
				FOR UPDATE
				SKIP LOCKED
			)
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		finishstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = $1
		`)
		if err != nil {
			return nil, err
		}

		return func(conn *sql.Conn) int {
			var itemid int
			err := grabstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				return -1
			} else if err != nil {
				panic(err)
			}
			_, err = finishstmt.Exec(itemid)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

// Same as skipLocked, but with a single transaction.
func skipLockedSingleTxn() initFunctionType {
	return func(conn *sql.Conn) (exec func(conn *sql.Conn) int, err error) {
		grabandmarkstmt, err := conn.Prepare(`
			UPDATE items
				SET processed = now()
			WHERE itemid = (
				SELECT itemid
				FROM items
				WHERE processed IS NULL
				ORDER BY itemid
				LIMIT 1
				FOR UPDATE
				SKIP LOCKED
			)
			RETURNING itemid
		`)
		if err != nil {
			return nil, err
		}
		// no race check necessary \:D/

		return func(conn *sql.Conn) int {
			var itemid int

			// XXX DO NOT DO THIS IN A REAL APP XXX
			_, err = conn.Exec(`BEGIN`)
			if err != nil {
				panic(err)
			}

			err := grabandmarkstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				_, _ = conn.Exec(`ROLLBACK`)
				return -1
			} else if err != nil {
				panic(err)
			}

			_, err = conn.Exec(`COMMIT`)
			if err != nil {
				panic(err)
			}
			return itemid
		}, err
	}
}

func initDatabase(numItems int, method string) *sql.DB {
	db, err := sql.Open("postgres", "host=/tmp sslmode=disable")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS items")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`
		CREATE TABLE items (
			itemid integer PRIMARY KEY,
			grabbed timestamptz,
			processed timestamptz
		)
	`)
	if err != nil {
		panic(err)
	}

	noGrabbed := false
	processedIndex := false

	// method-specific details
	switch method {
	case "naiveUpdate", "randomOffset", "skipLocked":
	case "advisoryLocksNaive", "advisoryLocksHold", "advisoryLocksRandomOffset":
	case "advisoryLocksBatchRelease", "advisoryLocksFixedLengthQueue":
		// nothing to do

	case "naiveUpdateSingleTxn", "advisoryLocksSingleTxn", "randomOffsetSingleTxn", "skipLockedSingleTxn", "advisoryLocksQue":
		noGrabbed = true
		processedIndex = true
	default:
		fmt.Fprintf(os.Stderr, "unrecognized method %s\n", method)
		os.Exit(1)
	}

	if noGrabbed {
		// only a "processed" column is necessary
		_, err = db.Exec(`ALTER TABLE items DROP COLUMN grabbed`)
		if err != nil {
			panic(err)
		}
	}

	// force a rewrite of the table
	_, err = db.Exec(`ALTER TABLE items ADD COLUMN payload text DEFAULT NULL`)
	if err != nil {
		panic(err)
	}

	ins, err := db.Prepare("INSERT INTO items (itemid, payload) VALUES ($1, $2)")
	if err != nil {
		panic(err)
	}
	for i := 0; i < numItems; i++ {
		_, err = ins.Exec(i, `
This life, which had been the tomb
of his virtue and of his honour, is
but a walking shadow.
`)
		if err != nil {
			panic(err)
		}
	}

	if !noGrabbed {
		_, err = db.Exec("CREATE INDEX on items(itemid) WHERE grabbed IS NULL")
		if err != nil {
			panic(err)
		}
	}
	if processedIndex {
		_, err = db.Exec("CREATE INDEX on items(itemid) WHERE processed IS NULL")
		if err != nil {
			panic(err)
		}
	}

	return db
}

func main() {
	var vacuum bool

	if len(os.Args) == 1 {
		vacuum = false
	} else if len(os.Args) == 2 {
		switch os.Args[1] {
		case "yes":
			vacuum = true
		case "no":
			vacuum = false
		default:
			fmt.Fprintf(os.Stderr, "unrecognized boolean value %q\n", os.Args[1])
			os.Exit(1)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Usage: %s [vacuum]\n", os.Args[0])
		os.Exit(1)
	}

	if vacuum {
		fmt.Fprintf(os.Stderr, "vacuum=yes is not supported\n")
		os.Exit(1)
	}

	tests := []struct{
		numConnections int
		numItems int
	}{
		{  1,	75000, },
		{  2,	75000, },
		{  4,	75000, },
		{  6,	75000, },
		{  8,	75000, },
		{ 12,	75000, },
		{ 16,	75000, },
	}

	methods := []string{
		"naiveUpdate",
		"naiveUpdateSingleTxn",
		"advisoryLocksNaive",
		"advisoryLocksHold",
		"advisoryLocksRandomOffset",
		"advisoryLocksBatchRelease",
		"advisoryLocksFixedLengthQueue",
		"advisoryLocksSingleTxn",
		"randomOffset",
		"randomOffsetSingleTxn",
		"skipLocked",
		"skipLockedSingleTxn",
	}

	p, err := plot.New()
	if err != nil {
		panic(err)
	}
	p.Title.Text = "Queue performance"
	p.X.Tick.Marker = func(min, max float64) []plot.Tick {
		return []plot.Tick{{0,"0"},{1,"1"},{2,"2"},{4,"4"},{6,"6"},{8,"8"},{10,""},{12,"12"},{14,""},{16,"16"}}
	}
	p.X.Label.Text = "Number of connections"
	p.X.Min = 0.0
	p.X.Max = 18.0
	p.Y.Min = 0.0
	p.Y.Label.Text = "Time spent per item"

	var linePoints []interface{}
	for _, method := range methods {
		fmt.Printf("\n\nMETHOD: %s\n\n", method)
		pts := make(plotter.XYs, len(tests))
		for ti, t := range tests {
			var fastest int64 = -1
			for i := 0; i < 3; i++ {
				nsecs := runTest(method, t.numItems, t.numConnections, vacuum)
				if fastest == -1 || nsecs < fastest {
					fastest = nsecs
				}
			}
			pts[ti].X = float64(t.numConnections)
			pts[ti].Y = (float64(fastest) / 1000.0) / float64(t.numItems)
			fmt.Println(pts[ti].Y)
			fmt.Println(fastest)
		}
		linePoints = append(linePoints, method, pts)
	}
	err = plotutil.AddLinePoints(p, linePoints...)
	if err != nil {
		panic(err)
	}

	err = p.Save(16.0, 11.0, "plot.png")
	if err != nil {
		panic(err)
	}
}

func runTest(method string, numItems int, numConnections int, vacuum bool) int64 {
	var initFunc initFunctionType

	switch method {
	case "naiveUpdate":
		initFunc = naiveUpdate()
	case "naiveUpdateSingleTxn":
		initFunc = naiveUpdateSingleTxn()
	case "advisoryLocksNaive":
		initFunc = advisoryLocksNaive()
	case "advisoryLocksQue":
		initFunc = advisoryLocksQue()
	case "advisoryLocksHold":
		initFunc = advisoryLocksHold()
	case "advisoryLocksRandomOffset":
		initFunc = advisoryLocksRandomOffset(numConnections)
	case "advisoryLocksBatchRelease":
		initFunc = advisoryLocksBatchRelease()
	case "advisoryLocksFixedLengthQueue":
		initFunc = advisoryLocksFixedLengthQueue()
	case "advisoryLocksSingleTxn":
		initFunc = advisoryLocksSingleTxn()
	case "randomOffset":
		initFunc = randomOffset(numConnections)
	case "randomOffsetSingleTxn":
		initFunc = randomOffsetSingleTxn(numConnections)
	case "skipLocked":
		initFunc = skipLocked()
	case "skipLockedSingleTxn":
		initFunc = skipLockedSingleTxn()
	default:
		fmt.Fprintf(os.Stderr, "unrecognized method %s\n", method)
		os.Exit(1)
	}

	db := initDatabase(numItems, method)
	defer db.Close()

	_, err := db.Exec("CHECKPOINT")
	if err != nil {
		panic(err)
	}

	startingPistol := make(chan struct{})
	dataChan := make(chan bitmap, numConnections)
	counterChan := make(chan int, numConnections)
	var wg sync.WaitGroup
	wg.Add(numConnections)

	for i := 0; i < numConnections; i++ {
		conn, err := db.AcquireConn()
		if err != nil {
			panic(err)
		}

		go func() {
			defer conn.Release()

			b := newBitmap(numItems)
			exec, err := initFunc(conn)
			if err != nil {
				panic(err)
			}

			wg.Done()
			<-startingPistol

			numItems := 0
			for {
				itemid := exec(conn)
				if itemid == -1 {
					break
				}
				b.set(itemid)
				numItems++
			}

			dataChan <- b
			counterChan <- numItems
			wg.Done()
		}()
	}

	wg.Wait()

	time.Sleep(2 * time.Second)
	runtime.GC()

	numRaces = 0

	wg.Add(numConnections)
	startTimer := time.Now()
	close(startingPistol)
	wg.Wait()
	endTimer := time.Now()

	// Check that the queries actually worked; all items processed exactly once
	var distribution []int
	b := newBitmap(numItems)
	for i := 0; i < numConnections; i++ {
		grb := <-dataChan
		b.merge(grb)
		distribution = append(distribution, <-counterChan)
	}
	b.mustBeAllOnes()

	// who needs generics
	fmt.Printf("distribution: ")
	for i, numItems := range distribution {
		fmt.Printf("%d", numItems)
		if i < len(distribution)-1 {
			fmt.Printf(", ")
		}
	}
	fmt.Printf(" (%d races)\n", numRaces)

	diff := endTimer.Sub(startTimer)
	return diff.Nanoseconds()
}
