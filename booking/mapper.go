package main

// screenConfig describes the layout of a single cinema screen and the base
// STM address for its seat range.
//
// Memory layout (256 addresses total):
//
//	Screen 1 → addresses   0–99   (10 rows × 10 cols)
//	Screen 2 → addresses 100–199  (10 rows × 10 cols)
//	Screen 3 → addresses 200–249  ( 5 rows × 10 cols)
//	Metadata → addresses 250–255
type screenConfig struct {
	Rows int
	Cols int
	Base uint32
}

var screenConfigs = map[int]screenConfig{
	1: {Rows: 10, Cols: 10, Base: 0},
	2: {Rows: 10, Cols: 10, Base: 100},
	3: {Rows: 5, Cols: 10, Base: 200},
}

// seatToAddr converts a (screenID, row, col) triple to a STM memory address.
// row and col are 0-indexed.  Returns (addr, true) on success, (0, false) if
// the seat coordinates are out of range for the given screen.
func seatToAddr(screenID, row, col int) (uint32, bool) {
	cfg, ok := screenConfigs[screenID]
	if !ok || row < 0 || row >= cfg.Rows || col < 0 || col >= cfg.Cols {
		return 0, false
	}
	return cfg.Base + uint32(row*cfg.Cols+col), true
}
