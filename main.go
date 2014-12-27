// Command-line interface to a remote DVID server.
// Provides essential commands on top of core http server: init, serve, repair.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	batchsize = flag.Int("batchsize", 16, "")
	blocksize = flag.Int("blocksize", 32, "")

	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Run in verbose mode if true.
	runVerbose = flag.Bool("verbose", false, "")
)

const helpMessage = `
partition reads a JSON-encoded list of block spans and creates subvolumes.

Usage: partition [options] <command>

      -batchsize  =number   Number of blocks along one axis of a substack (default 16)
      -blocksize  =number   Number of voxels along one axis of a block (default 32)
      -verbose    (flag)    Run in verbose mode.
  -h, -help       (flag)    Show help message

`

var usage = func() {
	fmt.Printf(helpMessage)
}

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

// Tuples are (Z, Y, X0, X1)
type Span [4]int

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() >= 1 && strings.ToLower(flag.Args()[0]) == "help" {
		*showHelp = true
	}

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Read in from stdin
	input, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Printf("Error in reading from standard input: %s", err.Error())
		os.Exit(1)
	}

	// Parse the JSON into spans
	spans := []Span{}
	if err := json.Unmarshal(input, &spans); err != nil {
		fmt.Printf("Error parsing JSON from stdin: %s\n", err.Error())
		os.Exit(1)
	}

	// Create a simple matrix of 100 x 100 x 100 subvolumes.  If span is within
	// a subvolume, it gets used.
	const (
		nz int = 400
		ny int = 400
		nx int = 400
	)
	var maxx, maxy, maxz int
	var numSubvolumes int
	var numActiveBlocks int
	var active [nz][ny][nx]int
	for _, span := range spans {
		z := span[0]
		y := span[1]
		x0 := span[2]
		x1 := span[3]

		gz := z / *batchsize
		gy := y / *batchsize
		if gy >= ny {
			fmt.Printf("Block y index (%d) exceeds static subvolume.", gy)
			os.Exit(1)
		}
		if gz >= nz {
			fmt.Printf("Block z index (%d) exceeds static subvolume.", gz)
			os.Exit(1)
		}
		if gz > maxz {
			maxz = gz
		}
		if gy > maxy {
			maxy = gy
		}
		for x := x0; x <= x1; x++ {
			gx := x / *batchsize
			if gx >= nx {
				fmt.Printf("Block x index (%d) exceeds static subvolume.", gx)
				os.Exit(1)
			}
			if gx > maxx {
				maxx = gx
			}
			if active[gz][gy][gx] == 0 {
				numSubvolumes++
			}
			active[gz][gy][gx]++
			numActiveBlocks++
		}
	}

	// Print all foreground subvolumes
	voxelwidth := *batchsize * *blocksize
	subvolumes := subvolumesT{
		numSubvolumes * *batchsize * *batchsize * *batchsize,
		numActiveBlocks,
		numSubvolumes,
		0,
		[]subvolumeT{},
	}
	subvolumes.Subvolumes = []subvolumeT{}
	var numPruned int
	for z := 0; z < nz; z++ {
		vz0 := z * voxelwidth
		vz1 := vz0 + voxelwidth - 1
		bz0 := vz0 / *blocksize
		bz1 := vz1 / *blocksize
		for y := 0; y < ny; y++ {
			vy0 := y * voxelwidth
			vy1 := vy0 + voxelwidth - 1
			by0 := vy0 / *blocksize
			by1 := vy1 / *blocksize
			for x := 0; x < nx; x++ {
				vx0 := x * voxelwidth
				vx1 := vx0 + voxelwidth - 1
				bx0 := vx0 / *blocksize
				bx1 := vx1 / *blocksize
				if active[z][y][x] > 0 {
					voxelExtent := Extents3d{
						Point3d{vx0, vy0, vz0},
						Point3d{vx1, vy1, vz1},
					}
					blockExtent := ChunkExtents3d{
						Point3d{bx0, by0, bz0},
						Point3d{bx1, by1, bz1},
					}
					subvol := subvolumeT{
						voxelExtent,
						blockExtent,
						*batchsize * *batchsize * *batchsize,
						active[z][y][x],
					}
					subvolumes.Subvolumes = append(subvolumes.Subvolumes, subvol)
				} else if z <= maxz && y <= maxy && x <= maxx {
					numPruned++
				}
			}
		}
	}
	subvolumes.SubvolsPruned = numPruned

	// Encode as JSON
	jsonBytes, err := json.MarshalIndent(subvolumes, "", "    ")
	if err != nil {
		fmt.Printf("Error turning partitioning into JSON: %s\n", err.Error())
	}
	fmt.Println(string(jsonBytes))
}

type Point3d [3]int

type subvolumesT struct {
	NumTotalBlocks  int
	NumActiveBlocks int
	NumSubvolumes   int
	SubvolsPruned   int
	Subvolumes      []subvolumeT
}

type subvolumeT struct {
	Extents3d
	ChunkExtents3d
	TotalBlocks  int
	ActiveBlocks int
}

// Extents defines a 3d volume
type Extents3d struct {
	MinPoint Point3d
	MaxPoint Point3d
}

// ChunkExtents3d defines a 3d volume of chunks
type ChunkExtents3d struct {
	MinChunk Point3d
	MaxChunk Point3d
}
