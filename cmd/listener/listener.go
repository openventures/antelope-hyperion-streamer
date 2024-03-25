package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/openventures/antelope-hyperion-streamer/internal/state"
	"github.com/openventures/antelope-hyperion-streamer/pkg/streamer"
)

var (
	endpointFlag      = flag.String("endpoint", "https://wax.greymass.com", "Hyperion Endpoint")
	statePathFlag     = flag.String("state-file", ".state", "State file")
	sleepTimeBaseFlag = flag.Duration("sleep-time-base", 30*time.Second, "base sleep time (for the exponential backoff)")
	filterFlag        = flag.String("filter", "", "url encoded filters to pass to the get_actions endpoint. e.g. 'account=eosio&act.name=buyrambytes'")
)

func main() {
	flag.Parse()
	if err := run(context.Background()); err != nil {
		slog.Error("error", "err", err)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ctx, done := signal.NotifyContext(ctx, os.Interrupt)
	defer done()

	st, err := state.Load(ctx, *statePathFlag)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	slog.Info("booting from", "global_sequence", st.HeadGlobalSequence)
	if *sleepTimeBaseFlag < 1*time.Second {
		return fmt.Errorf("sleep-time-base must be at least 1s")
	}

	filters, err := getFilter()
	if err != nil {
		return fmt.Errorf("get filter: %w", err)
	}
	if len(filters) == 0 {
		return fmt.Errorf("at least one filter is required")
	}

	s := streamer.Streamer{
		BaseURL:   strings.TrimRight(*endpointFlag, "/"),
		PageLimit: 1000,
		Logger:    slog.Default(),
		SleepBase: *sleepTimeBaseFlag,
	}

	batches, errC := s.Start(ctx, st.HeadGlobalSequence, filters)

	lastPrintHeadBlock := uint32(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errC:
			return fmt.Errorf("loader stopped: %w", err)
		case b := <-batches:
			var out []string
			for _, raw := range b.Actions {
				out = append(out, string(raw))
			}
			fmt.Print(strings.Join(out, "\n") + "\n")
			st.HeadGlobalSequence = b.HeadGlobalSequence
			if err := state.Store(ctx, *statePathFlag, st); err != nil {
				return fmt.Errorf("store state: %w", err)
			}
			if b.HeadBlock-lastPrintHeadBlock > 500 {
				slog.Info("current height", "block_num", b.HeadBlock)
				lastPrintHeadBlock = b.HeadBlock
			}
		}
	}
}

func getFilter() ([]streamer.Filter, error) {
	values, err := url.ParseQuery(*filterFlag)
	if err != nil {
		return nil, fmt.Errorf("parse filter: %w", err)
	}
	var filters []streamer.Filter
	for k, v := range values {
		if len(v) != 1 {
			return nil, fmt.Errorf("%q contains multiple values, only one is allowed", k)
		}
		filters = append(filters, streamer.Filter{
			Key:   k,
			Value: v[0],
		})
	}
	return filters, nil
}
