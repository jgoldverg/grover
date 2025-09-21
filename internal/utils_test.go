package internal

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type fakeCredential struct {
	name string
	typ  string
	url  string
	id   uuid.UUID
}

func (f *fakeCredential) GetName() string    { return f.name }
func (f *fakeCredential) GetType() string    { return f.typ }
func (f *fakeCredential) GetUrl() string     { return f.url }
func (f *fakeCredential) GetUUID() uuid.UUID { return f.id }
func (f *fakeCredential) Validate() error    { return nil }

type fakeCredentialStorage struct {
	byUUID map[uuid.UUID]backend.Credential
	byName map[string]backend.Credential
}

func (f *fakeCredentialStorage) GetCredentialByUUID(id uuid.UUID) (backend.Credential, error) {
	cred, ok := f.byUUID[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return cred, nil
}

func (f *fakeCredentialStorage) GetCredentialByName(name string) (backend.Credential, error) {
	cred, ok := f.byName[name]
	if !ok {
		return nil, errors.New("not found")
	}
	return cred, nil
}

func (f *fakeCredentialStorage) AddCredential(backend.Credential) error         { return nil }
func (f *fakeCredentialStorage) DeleteCredential(uuid.UUID) error               { return nil }
func (f *fakeCredentialStorage) DeleteCredentialByName(string) error            { return nil }
func (f *fakeCredentialStorage) ListCredentials() ([]backend.Credential, error) { return nil, nil }
func (f *fakeCredentialStorage) ListCredentialsByType(string) ([]backend.Credential, error) {
	return nil, nil
}

func TestBuildCredentialRef(t *testing.T) {
	id := uuid.New()
	cref := BuildCredentialRef("ignored", id)
	ref, ok := cref.Ref.(*pb.CredentialRef_CredentialUuid)
	if !ok || ref.CredentialUuid != id.String() {
		t.Fatalf("expected credential uuid %s, got %#v", id, cref.Ref)
	}

	nameRef := BuildCredentialRef("my-name", uuid.Nil)
	if _, ok := nameRef.Ref.(*pb.CredentialRef_CredentialName); !ok {
		t.Fatalf("expected name based ref, got %#v", nameRef.Ref)
	}
}

func TestCredentialTypeConversions(t *testing.T) {
	if got := CredentialTypeToPbType(&fakeCredential{typ: "basic"}); got != pb.CredentialType_BASIC_CREDENTIAL_TYPE {
		t.Fatalf("unexpected pb type for basic")
	}
	if got := CredentialTypeToPbType(&fakeCredential{typ: "ssh"}); got != pb.CredentialType_SSH_CREDENTIAL_TYPE {
		t.Fatalf("unexpected pb type for ssh")
	}
	if got := CredentialTypeToPbType(&fakeCredential{typ: "other"}); got != pb.CredentialType_CREDENTIAL_TYPE_UNSPECIFIED {
		t.Fatalf("expected unspecified for unknown types")
	}

	if got := BackendTypeToPbType(backend.LOCALFSBackend); got != pb.EndpointType_LOCAL_FS {
		t.Fatalf("unexpected endpoint type for localfs")
	}
}

func TestResolveCredentialByUUIDAndName(t *testing.T) {
	id := uuid.New()
	cred := &fakeCredential{name: "cred", typ: "basic", id: id}
	storage := &fakeCredentialStorage{
		byUUID: map[uuid.UUID]backend.Credential{id: cred},
		byName: map[string]backend.Credential{"cred": cred},
	}

	uuidRef := &pb.CredentialRef{Ref: &pb.CredentialRef_CredentialUuid{CredentialUuid: id.String()}}
	resolved, err := ResolveCredential(storage, uuidRef)
	if err != nil || resolved != cred {
		t.Fatalf("expected uuid lookup to succeed, err=%v", err)
	}

	nameRef := &pb.CredentialRef{Ref: &pb.CredentialRef_CredentialName{CredentialName: "cred"}}
	resolved, err = ResolveCredential(storage, nameRef)
	if err != nil || resolved != cred {
		t.Fatalf("expected name lookup to succeed, err=%v", err)
	}
}
