package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

// NewRouter returns an http.Handler with all booking routes registered.
func NewRouter(svc *BookingService) http.Handler {
	mux := http.NewServeMux()

	// GET /screens — list all available screens.
	mux.HandleFunc("GET /screens", handleListScreens)

	// GET /screens/{id}/seats — consistent snapshot of all seats on a screen.
	mux.HandleFunc("GET /screens/{id}/seats", func(w http.ResponseWriter, r *http.Request) {
		handleGetSeats(w, r, svc)
	})

	// POST /book — atomically book one or more seats.
	mux.HandleFunc("POST /book", func(w http.ResponseWriter, r *http.Request) {
		handleBook(w, r, svc)
	})

	// POST /cancel — release a seat previously booked by the caller.
	mux.HandleFunc("POST /cancel", func(w http.ResponseWriter, r *http.Request) {
		handleCancel(w, r, svc)
	})

	return mux
}

// ── helpers ──────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ── handlers ─────────────────────────────────────────────────────────────────

type screenInfo struct {
	ID   int `json:"id"`
	Rows int `json:"rows"`
	Cols int `json:"cols"`
}

func handleListScreens(w http.ResponseWriter, _ *http.Request) {
	list := make([]screenInfo, 0, len(screenConfigs))
	for id, cfg := range screenConfigs {
		list = append(list, screenInfo{ID: id, Rows: cfg.Rows, Cols: cfg.Cols})
	}
	writeJSON(w, http.StatusOK, list)
}

type seatsResponse struct {
	ScreenID int        `json:"screen_id"`
	Rows     int        `json:"rows"`
	Cols     int        `json:"cols"`
	Seats    [][]uint32 `json:"seats"`
}

func handleGetSeats(w http.ResponseWriter, r *http.Request, svc *BookingService) {
	screenID, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid screen id")
		return
	}

	cfg, ok := screenConfigs[screenID]
	if !ok {
		writeError(w, http.StatusNotFound, "screen not found")
		return
	}

	grid, err := svc.GetSeats(screenID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, seatsResponse{
		ScreenID: screenID,
		Rows:     cfg.Rows,
		Cols:     cfg.Cols,
		Seats:    grid,
	})
}

type bookRequest struct {
	UserID   uint32 `json:"user_id"`
	ScreenID int    `json:"screen_id"`
	Seats    []Seat `json:"seats"`
}

type commitResponse struct {
	CommitTimestamp int64 `json:"commit_timestamp"`
}

func handleBook(w http.ResponseWriter, r *http.Request, svc *BookingService) {
	var req bookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.UserID == 0 {
		writeError(w, http.StatusBadRequest, "user_id must be non-zero")
		return
	}
	if len(req.Seats) == 0 {
		writeError(w, http.StatusBadRequest, "seats must not be empty")
		return
	}

	ts, err := svc.Book(req.UserID, req.ScreenID, req.Seats)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, ErrInvalidSeat) {
			status = http.StatusBadRequest
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, commitResponse{CommitTimestamp: ts})
}

type cancelRequest struct {
	UserID   uint32 `json:"user_id"`
	ScreenID int    `json:"screen_id"`
	Seat     Seat   `json:"seat"`
}

func handleCancel(w http.ResponseWriter, r *http.Request, svc *BookingService) {
	var req cancelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.UserID == 0 {
		writeError(w, http.StatusBadRequest, "user_id must be non-zero")
		return
	}

	ts, err := svc.Cancel(req.UserID, req.ScreenID, req.Seat)
	if err != nil {
		status := http.StatusConflict
		switch {
		case errors.Is(err, ErrInvalidSeat):
			status = http.StatusBadRequest
		case errors.Is(err, ErrSeatFree):
			status = http.StatusNotFound
		case errors.Is(err, ErrNotOwner):
			status = http.StatusForbidden
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, commitResponse{CommitTimestamp: ts})
}
