package internal

import (
	"strings"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/filesystem"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func PbFilesToFsFiles(in []*pb.FileInfo) []filesystem.FileInfo {
	out := make([]filesystem.FileInfo, 0, len(in))
	for _, f := range in {
		out = append(out, filesystem.FileInfo{
			ID:      f.GetId(),
			AbsPath: f.GetPath(),
			Size:    f.GetSize(),
		})
	}
	return out
}

func PbCredentialToBackendCredential(in []*pb.Credential) []backend.Credential {
	out := make([]backend.Credential, 0, len(in))
	for _, cred := range in {
		id, _ := uuid.Parse(cred.GetCredentialUuid())
		switch d := cred.Details.(type) {
		case *pb.Credential_Basic:
			b := &backend.BasicAuthCredential{
				Name:     cred.GetCredentialName(),
				Username: d.Basic.GetUsername(),
				Password: d.Basic.GetPassword(),
				URL:      d.Basic.GetUrl(),
				UUID:     id,
			}
			out = append(out, b)

		case *pb.Credential_Ssh:
			s := &backend.SSHCredential{
				Name:       cred.GetCredentialName(),
				Username:   d.Ssh.GetUsername(),
				Host:       d.Ssh.GetHost(),
				Port:       int(d.Ssh.GetPort()), // default handled by Validate()
				PublicKey:  d.Ssh.GetPublicKey(),
				PrivateKey: d.Ssh.GetPrivateKey(),
				UseAgent:   d.Ssh.GetUseAgent(),
				UUID:       id,
			}
			out = append(out, s)
		}
	}
	return out
}

func BuildCredentialRef(name string, id uuid.UUID) *pb.CredentialRef {
	cref := &pb.CredentialRef{}
	name = strings.TrimSpace(name)
	if id != uuid.Nil {
		cref = &pb.CredentialRef{
			Ref: &pb.CredentialRef_CredentialUuid{CredentialUuid: id.String()},
		}
	} else if name != "" {
		cref = &pb.CredentialRef{
			Ref: &pb.CredentialRef_CredentialName{CredentialName: name},
		}
	}
	return cref
}

func CredentialTypeToPbType(c backend.Credential) pb.CredentialType {
	t := c.GetType()
	if t == "basic" {
		return pb.CredentialType_BASIC_CREDENTIAL_TYPE
	}
	if t == "ssh" {
		return pb.CredentialType_SSH_CREDENTIAL_TYPE
	}
	return pb.CredentialType_CREDENTIAL_TYPE_UNSPECIFIED
}

func BackendTypeToPbType(t backend.BackendType) pb.EndpointType {
	switch t {
	case backend.LOCALFSBackend:
		return pb.EndpointType_LOCAL_FS
	case backend.HTTPBackend:
		return pb.EndpointType_HTTP
	default:
		return pb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED
	}
}

func ResolveCredential(storage backend.CredentialStorage, ref *pb.CredentialRef) (backend.Credential, error) {
	if ref == nil || ref.Ref == nil {
		return nil, nil
	}

	switch r := ref.Ref.(type) {
	case *pb.CredentialRef_CredentialUuid:
		u, err := uuid.Parse(r.CredentialUuid)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid credential UUID %q: %v", r.CredentialUuid, err)
		}
		cred, err := storage.GetCredentialByUUID(u)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "credential uuid %q: %v", r.CredentialUuid, err)
		}
		return cred, nil

	case *pb.CredentialRef_CredentialName:
		name := strings.TrimSpace(r.CredentialName)
		if name == "" {
			return nil, nil // treat empty name as "no ref"
		}
		cred, err := storage.GetCredentialByName(name)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "credential name %q: %v", name, err)
		}
		return cred, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown credential_ref variant")
	}
}
