package durex_test

import (
	"testing"

	"github.com/simonovic86/durex"
)

func TestRegistry_Register(t *testing.T) {
	r := durex.NewRegistry()
	cmd := &TestCommand{name: "test"}

	err := r.Register(cmd)
	if err != nil {
		t.Fatalf("first register should succeed: %v", err)
	}

	err = r.Register(cmd)
	if err == nil {
		t.Error("duplicate register should return error")
	}
}

func TestRegistry_RegisterEmptyName(t *testing.T) {
	r := durex.NewRegistry()
	cmd := &TestCommand{name: ""}

	err := r.Register(cmd)
	if err == nil {
		t.Error("register with empty name should return error")
	}
}

func TestRegistry_MustRegisterPanicsOnDuplicate(t *testing.T) {
	r := durex.NewRegistry()
	cmd := &TestCommand{name: "test"}
	r.MustRegister(cmd)

	defer func() {
		if rec := recover(); rec == nil {
			t.Error("MustRegister duplicate should panic")
		}
	}()
	r.MustRegister(cmd)
}

func TestRegistry_MustRegisterPanicsOnEmptyName(t *testing.T) {
	r := durex.NewRegistry()
	cmd := &TestCommand{name: ""}

	defer func() {
		if rec := recover(); rec == nil {
			t.Error("MustRegister empty name should panic")
		}
	}()
	r.MustRegister(cmd)
}

func TestRegistry_Overwrite(t *testing.T) {
	r := durex.NewRegistry()
	cmd1 := &TestCommand{name: "test"}
	cmd2 := &TestCommand{name: "test"}

	r.MustRegister(cmd1)
	r.Overwrite(cmd2) // should not panic

	resolved, err := r.Resolve("test")
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if resolved != cmd2 {
		t.Error("Overwrite should replace the handler")
	}
}

func TestRegistry_OverwriteNew(t *testing.T) {
	r := durex.NewRegistry()
	cmd := &TestCommand{name: "new"}

	r.Overwrite(cmd) // should work even if not previously registered

	resolved, err := r.Resolve("new")
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if resolved != cmd {
		t.Error("Overwrite should add new handler")
	}
}
