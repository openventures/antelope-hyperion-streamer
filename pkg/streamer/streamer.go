package streamer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Filter struct {
	Key   string
	Value string
}

type Batch struct {
	HeadGlobalSequence uint64
	HeadBlock          uint32
	Actions            []json.RawMessage
}

type Streamer struct {
	Client    http.Client
	BaseURL   string
	PageLimit int

	Logger *slog.Logger

	SleepBase time.Duration
}

func (s Streamer) Start(ctx context.Context, from uint64, filters []Filter) (<-chan Batch, <-chan error) {
	send := make(chan Batch)
	sendErr := make(chan error)
	go func() {
		defer close(send)
		defer close(sendErr)
		err := (func() error {

			globalSequenceLowerBound := from
			globalSequenceUpperBound := uint64(math.MaxInt64)
			numWaitsSinceCatchup := uint(0)

			for {
				limit := s.PageLimit
				if limit == 0 {
					limit = 100
				}
				q := make(url.Values)
				q.Add("sort", "asc")
				q.Add("limit", fmt.Sprint(limit))
				q.Add("global_sequence", fmt.Sprintf("%d-%d", globalSequenceLowerBound+1, globalSequenceUpperBound))
				for _, f := range filters {
					q.Add(f.Key, f.Value)
				}

				targetURL := fmt.Sprintf("%s/v2/%s/%s?%s", s.BaseURL, "history", "get_actions", q.Encode())
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
				if err != nil {
					return err
				}

				resp, err := s.Client.Do(req)
				if err != nil {
					return fmt.Errorf("http request: %w", err)
				}
				if resp.StatusCode == 429 {
					wait := 20 * time.Second
					if s.Logger != nil {
						s.Logger.Info("rate limited", "wait", wait)
					}
					err := sleepWithContext(ctx, wait)
					if err != nil {
						return err
					}
					continue
				}
				if resp.StatusCode >= 400 {
					return fmt.Errorf("http error: status=%d", resp.StatusCode)
				}

				var res getActionsResponse
				err = json.NewDecoder(resp.Body).Decode(&res)
				if err != nil {
					return fmt.Errorf("decode body: %w", err)
				}

				var b Batch
				for _, act := range res.Actions {
					globalSequence, err := getGlobalSequence(act)
					if err != nil {
						return fmt.Errorf("get global_sequence: %w", err)
					}
					if globalSequence > b.HeadGlobalSequence {
						b.HeadGlobalSequence = globalSequence
						b.HeadBlock = act.BlockNum
					}
					b.Actions = append(b.Actions, act.Act)
				}

				if b.HeadGlobalSequence == globalSequenceLowerBound || len(res.Actions) == 0 {
					// all caught up. do exponential backoff.
					numWaitsSinceCatchup++
					if numWaitsSinceCatchup > 10 {
						numWaitsSinceCatchup = 10
					}
					base := s.SleepBase
					if base < 10*time.Millisecond {
						base = 1 * time.Second
					}
					wait := time.Duration(numWaitsSinceCatchup*numWaitsSinceCatchup) * base
					if s.Logger != nil {
						s.Logger.Info("sleep", "wait", wait)
					}
					err := sleepWithContext(ctx, wait)
					if err != nil {
						return err
					}
					continue
				} else {
					numWaitsSinceCatchup = 0
				}

				if b.HeadGlobalSequence > globalSequenceLowerBound {
					globalSequenceLowerBound = b.HeadGlobalSequence
				} else {
					return fmt.Errorf("got actions with global_sequence=%d out of order head=%d", b.HeadGlobalSequence, globalSequenceLowerBound)
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					send <- b
				}
			}
		})()
		if err != nil {
			sendErr <- err
		}
	}()

	return send, sendErr
}

type getActionsResponse struct {
	QueryTimeMs      float32 `json:"query_time_ms"`
	Cached           bool    `json:"cached"`
	LastIndexedBlock uint32  `json:"last_indexed_block"`
	Total            struct {
		Value    int    `json:"value"`
		Relation string `json:"relation"`
	} `json:"total"`
	Actions []getActionsResponseAction `json:"actions"`
}

type getActionsResponseAction struct {
	BlockNum       uint32          `json:"block_num"`
	GlobalSequence any             `json:"global_sequence"`
	Act            json.RawMessage `json:"act"`
}

func getGlobalSequence(act getActionsResponseAction) (uint64, error) {
	if gsStr, ok := act.GlobalSequence.(string); ok {
		globalSequence, err := strconv.ParseUint(gsStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse global sequence: %w", err)
		}
		return globalSequence, nil
	}
	if gsF, ok := act.GlobalSequence.(float64); ok {
		return uint64(gsF), nil
	}
	return 0, fmt.Errorf("unexpected type for global_sequence: %T", act.GlobalSequence)
}
