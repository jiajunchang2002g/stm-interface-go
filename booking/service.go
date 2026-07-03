package main

import (
	"assign2/submission"
	"errors"
)

// Sentinel errors returned by BookingService methods.
var (
	ErrSeatTaken   = errors.New("one or more seats are already booked")
	ErrNotOwner    = errors.New("seat is not booked by this user")
	ErrInvalidSeat = errors.New("invalid seat or screen")
	ErrMaxRetries  = errors.New("booking conflict: please try again")
)

const maxRetries = 10

// Seat identifies a cinema seat by its 0-indexed row and column.
type Seat struct {
	Row int `json:"row"`
	Col int `json:"col"`
}

// BookingService provides transactional seat-booking operations backed by the
// STM interface.
type BookingService struct {
	stm *submission.StmInterface
}

// NewBookingService wraps an initialised StmInterface.
func NewBookingService(stm *submission.StmInterface) *BookingService {
	return &BookingService{stm: stm}
}

// GetSeats returns a 2-D grid of seat values for screenID.
// A value of 0 means the seat is free; any other value is the userID that
// holds the booking.  The read is a serialisable snapshot: all rows are read
// inside a single STM transaction.
func (s *BookingService) GetSeats(screenID int) ([][]uint32, error) {
	cfg, ok := screenConfigs[screenID]
	if !ok {
		return nil, ErrInvalidSeat
	}

	for i := 0; i < maxRetries; i++ {
		tx := s.stm.Begin()
		grid := make([][]uint32, cfg.Rows)
		for row := 0; row < cfg.Rows; row++ {
			grid[row] = make([]uint32, cfg.Cols)
			for col := 0; col < cfg.Cols; col++ {
				addr, _ := seatToAddr(screenID, row, col)
				grid[row][col] = tx.Read(addr)
			}
		}
		if _, err := tx.Commit(); err == nil {
			return grid, nil
		}
	}
	return nil, ErrMaxRetries
}

// Book atomically books all seats in the slice for userID on the given screen.
// If any seat is already booked the operation is aborted and ErrSeatTaken is
// returned.  The method retries automatically on transient OCC conflicts.
func (s *BookingService) Book(userID uint32, screenID int, seats []Seat) (int64, error) {
	// Validate all coordinates before touching the STM.
	for _, seat := range seats {
		if _, ok := seatToAddr(screenID, seat.Row, seat.Col); !ok {
			return 0, ErrInvalidSeat
		}
	}

	for i := 0; i < maxRetries; i++ {
		tx := s.stm.Begin()

		// Read all requested seats; fail fast if any is taken.
		for _, seat := range seats {
			addr, _ := seatToAddr(screenID, seat.Row, seat.Col)
			if tx.Read(addr) != 0 {
				return 0, ErrSeatTaken
			}
		}

		// Stage writes for every seat.
		for _, seat := range seats {
			addr, _ := seatToAddr(screenID, seat.Row, seat.Col)
			tx.Write(addr, userID)
		}

		ts, err := tx.Commit()
		if err == nil {
			return ts, nil
		}
		// ErrConflict means another transaction touched one of the same addresses
		// between our read and our commit.  Retry with fresh reads.
	}
	return 0, ErrMaxRetries
}

// Cancel releases the booking held by userID for the given seat.
// ErrNotOwner is returned if the seat is free or held by a different user.
func (s *BookingService) Cancel(userID uint32, screenID int, seat Seat) (int64, error) {
	addr, ok := seatToAddr(screenID, seat.Row, seat.Col)
	if !ok {
		return 0, ErrInvalidSeat
	}

	for i := 0; i < maxRetries; i++ {
		tx := s.stm.Begin()

		if tx.Read(addr) != userID {
			return 0, ErrNotOwner
		}
		tx.Write(addr, 0)

		ts, err := tx.Commit()
		if err == nil {
			return ts, nil
		}
	}
	return 0, ErrMaxRetries
}
