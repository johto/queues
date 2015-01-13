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
	"sync"
	"time"
)

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

// Skip locked rows using advisory locks when grabbing an item
func advisoryLocks() initFunctionType {
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
					LIMIT 128
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

// Skip locked rows using advisory locks when grabbing an item; single txn
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
					LIMIT 128
				) potential_items
				WHERE pg_try_advisory_xact_lock(itemid)
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

		return func(conn *sql.Conn) int {
			var itemid int

			// XXX DO NOT DO THIS IN A REAL APP XXX
			_, err = conn.Exec(`BEGIN`)
			if err != nil {
				panic(err)
			}

		again:
			err := grabandmarkstmt.QueryRow().Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
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
			err := grabstmt.QueryRow(rand.Intn(numConnections)).Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
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
			err := grabandmarkstmt.QueryRow(rand.Intn(numConnections)).Scan(&itemid)
			if err == sql.ErrNoRows {
				var notDone bool
				err = racecheckstmt.QueryRow().Scan(&notDone)
				if err != nil {
					panic(err)
				}
				if (notDone) {
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
	case "naiveUpdate", "advisoryLocks", "skipLocked", "randomOffset":
		// nothing to do
	case "naiveUpdateSingleTxn", "advisoryLocksSingleTxn", "skipLockedSingleTxn", "randomOffsetSingleTxn":
		noGrabbed = true
		processedIndex = true
	default:
		fmt.Fprintf(os.Stderr, "unrecognized method %s\n", os.Args[1])
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
		vacuum = true
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

	tests := []struct{
		numConnections int
		numItems int
	}{
		{  1,	100000, },
		{  2,	250000, },
		{  4,	300000, },
		{  6,   300000, },
		{  8,	300000, },
		{ 12,	300000, },
		{ 16,	300000, },
	}

	methods := []string{
		"naiveUpdate",
		"naiveUpdateSingleTxn",
		"advisoryLocks",
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
	p.Y.Label.Text = "Time to spent per item"

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
	case "advisoryLocks":
		initFunc = advisoryLocks()
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

				_, err = conn.Exec("VACUUM items")
				if err != nil {
					panic(err)
				}
			}

			dataChan <- b
			counterChan <- numItems
			wg.Done()
		}()
	}

	wg.Wait()

	time.Sleep(5 * time.Second)

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
	fmt.Printf("\n")

	diff := endTimer.Sub(startTimer)
	return diff.Nanoseconds()
}
