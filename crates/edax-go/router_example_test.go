package edax_test

import (
	"context"
	"fmt"

	edax "github.com/haskian/edax/crates/edax-go"
)

func ExampleTypedEventHandler() {
	type Credited struct {
		AccountID string `json:"account_id"`
		Amount    string `json:"amount"`
		Currency  string `json:"currency"`
	}
	codec, err := edax.NewJSONCodec(map[string]func() any{
		"AccountCredited": func() any { return new(Credited) },
	})
	if err != nil {
		panic(err)
	}
	router := edax.NewEventRouter()
	handler, err := edax.TypedEventHandler(codec, func(_ context.Context, event *Credited) error {
		fmt.Printf("%s: %s %s\n", event.AccountID, event.Amount, event.Currency)
		return nil
	})
	if err != nil {
		panic(err)
	}
	if err := router.Register("AccountCredited", handler); err != nil {
		panic(err)
	}
	message := edax.EventMessage{
		ID: "credit-1", AggregateID: "account-1", Name: "AccountCredited", Version: 1,
		Data:          []byte(`{"account_id":"account-1","amount":"12.5","currency":"EUR"}`),
		CorrelationID: "deposit-1", CausationID: "command-1",
	}
	if err := router.Dispatch(context.Background(), message); err != nil {
		panic(err)
	}
	// Output: account-1: 12.5 EUR
}
