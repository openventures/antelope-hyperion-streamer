# antelope-hyperion-streamer

Listen to certain actions, with persitant checkpoint saving to allow for a gap free stream of actions (based on hyperion).

This package also contains an example stdout listener which can be used to store action traces in local files:

```bash
go run cmd/listener/listener.go -filter="account=eosio&act.name=sellram" | jq --unbuffered -r '.data.bytes' >> bytes.txt
```

This will store the amount of bytes sold in a file called `bytes.txt`, this also shows the power of combining this listener with [jq](https://jqlang.github.io/jq/) to process the action traces.
Once the action traces is printed to stdout, the last `global_sequence` is stored in a state file located at `.state`.

## Usage in your project

For experiments storing action traces in files, processed via shell pipes is fine, but for usage in serious projects, you should store the `global_sequence` state along side the processed actions and commit both atomically to your store.


```go
package main

import (
    "context"

    "github.com/openventures/antelope-hyperion-streamer/streamer"
)

func main() {
    ctx, cancel := context.WithCancel(context.TODO())
    defer cancel()

    head := DB.LoadHeadGlobalSequence(ctx)

    s := streamer.Streamer{
		BaseURL:   "https://some-hyperion-endpoint.example.com",
		PageLimit: 100,
	}
    batches, errC := s.Start(ctx, head, []streamer.Filter{{ Key: "account", Value: "eosio" }})

    for {
        select {
        case <-ctx.Done():
            return
        case err := <-errC:
            panic(fmt.Sprintf("loader stopped: %s", err.Error()))
        case b := <-batches:
            DB.InTransaction(ctx, func (tx *TX) {
                tx.StoreActions(b.Actions)
                tx.StoreHeadGlobalSequence(b.HeadGlobalSequence)
            })
        }
    }
}
```

This allows you to always restart the streamer and ensure a gap free list of actions.