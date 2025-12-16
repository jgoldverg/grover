package gserver

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type CredentialService struct {
	pb.UnimplementedCredentialServiceServer
	storage backend.CredentialStorage
}

func NewCredentialOps(credStore backend.CredentialStorage) *CredentialService {
	return &CredentialService{
		UnimplementedCredentialServiceServer: pb.UnimplementedCredentialServiceServer{},
		storage:                              credStore,
	}
}

func (co *CredentialService) List(ctx context.Context, in *pb.ListCredentialsRequest) (*pb.ListCredentialsResponse, error) {
	var credentials []*pb.Credential
	internal.Info("server listing credentials", nil)
	if in.GetType() == pb.CredentialType_CREDENTIAL_TYPE_UNSPECIFIED {
		creds, err := co.storage.ListCredentials()
		if err != nil {
			return nil, err
		}
		for _, cred := range creds {
			c := toProtoCredential(cred)
			credentials = append(credentials, c)
		}
		return &pb.ListCredentialsResponse{Credentials: credentials}, nil
	} else if in.GetType() == pb.CredentialType_BASIC_CREDENTIAL_TYPE || in.GetType() == pb.CredentialType_SSH_CREDENTIAL_TYPE {
		creds, err := co.storage.ListCredentialsByType(convertCredType(in.GetType()))
		if err != nil {
			return nil, err
		}
		for _, cred := range creds {
			c := toProtoCredential(cred)
			credentials = append(credentials, c)
		}
		return &pb.ListCredentialsResponse{Credentials: credentials}, nil
	} else {
		return nil, fmt.Errorf("unsupported credential type: %s", in.GetType())
	}
}

func (co *CredentialService) Create(ctx context.Context, in *pb.CreateCredentialRequest) (*pb.CreateCredentialResponse, error) {
	internal.Info("Create Credential: ", internal.Fields{
		"cred_pb_req": in,
	})
	cred := toBackendCredential(in.GetCredential())
	internal.Info("Backend Credential: ", internal.Fields{
		"cred": cred,
	})
	err := co.storage.AddCredential(cred)
	if err != nil {
		return nil, err
	}
	return &pb.CreateCredentialResponse{}, nil
}

func (co *CredentialService) Delete(ctx context.Context, in *pb.DeleteCredentialRequest) (*pb.DeleteCredentialResponse, error) {
	if in.GetRef().GetCredentialUuid() != "" {
		credUUID, err := uuid.Parse(in.GetRef().GetCredentialUuid())
		if err != nil {
			return nil, err
		}
		err = co.storage.DeleteCredential(credUUID)
		if err != nil {
			return nil, err
		}
	}
	if in.GetRef().GetCredentialName() != "" {
		err := co.storage.DeleteCredentialByName(in.GetRef().GetCredentialName())
		if err != nil {
			return nil, err
		}
	}

	return &pb.DeleteCredentialResponse{}, nil
}

func (co *CredentialService) Get(ctx context.Context, in *pb.GetCredentialRequest) (*pb.GetCredentialResponse, error) {
	if in.GetRef().GetCredentialUuid() == "" && in.GetRef().GetCredentialName() == "" {
		return nil, fmt.Errorf("no credential uuid or credential name")
	}
	if in.GetRef().GetCredentialUuid() != "" {
		credUUID, err := uuid.Parse(in.GetRef().GetCredentialUuid())
		if err != nil {
			return nil, err
		}
		cred, err := co.storage.GetCredentialByUUID(credUUID)
		if err != nil {
			return nil, err
		}
		return &pb.GetCredentialResponse{Credential: toProtoCredential(cred)}, nil
	}

	if in.GetRef().GetCredentialName() != "" {
		cred, err := co.storage.GetCredentialByName(in.GetRef().GetCredentialName())
		if err != nil {
			return nil, err
		}
		return &pb.GetCredentialResponse{Credential: toProtoCredential(cred)}, nil
	}
	return nil, nil
}

func convertCredType(credentialType pb.CredentialType) string {
	switch credentialType {
	case pb.CredentialType_BASIC_CREDENTIAL_TYPE:
		return "basic"
	case pb.CredentialType_SSH_CREDENTIAL_TYPE:
		return "ssh"
	default:
		return ""
	}
}

func toBackendCredential(cred *pb.Credential) backend.Credential {
	if cred == nil {
		return nil
	}

	credUUID := uuid.New()
	if id := cred.GetCredentialUuid(); id != "" {
		if parsed, err := uuid.Parse(id); err == nil {
			credUUID = parsed
		}
	}

	if pbCred := cred.GetSsh(); pbCred != nil {
		sshCred := backend.SSHCredential{
			Name:           cred.GetCredentialName(),
			Username:       pbCred.GetUsername(),
			Host:           pbCred.GetHost(),
			Port:           int(pbCred.GetPort()),
			PrivateKeyPath: "",
			PublicKeyPath:  "",
			PrivateKey:     pbCred.GetPrivateKey(),
			PublicKey:      pbCred.GetPublicKey(),
			UUID:           credUUID,
			UseAgent:       pbCred.GetUseAgent(),
		}
		return &sshCred
	}

	if pbCred := cred.GetBasic(); pbCred != nil {
		basicCred := backend.BasicAuthCredential{
			Name:     cred.GetCredentialName(),
			Username: pbCred.GetUsername(),
			Password: pbCred.GetPassword(),
			URL:      pbCred.GetUrl(),
			UUID:     credUUID,
		}
		return &basicCred
	}

	// fallback on the type field if no details are defined
	switch cred.GetType() {
	case pb.CredentialType_SSH_CREDENTIAL_TYPE:
		return &backend.SSHCredential{
			Name: cred.GetCredentialName(),
			UUID: credUUID,
		}
	case pb.CredentialType_BASIC_CREDENTIAL_TYPE:
		return &backend.BasicAuthCredential{
			Name: cred.GetCredentialName(),
			UUID: credUUID,
		}
	default:
		return nil
	}
}

func toProtoCredential(cred backend.Credential) *pb.Credential {
	switch c := cred.(type) {
	case *backend.BasicAuthCredential:
		return &pb.Credential{
			CredentialUuid: c.GetUUID().String(),
			CredentialName: c.GetName(),
			Type:           pb.CredentialType_BASIC_CREDENTIAL_TYPE,
			Details: &pb.Credential_Basic{
				Basic: &pb.BasicDetails{
					Username: c.GetUserName(),
					Password: c.GetPassword(),
					Url:      c.GetUrl(),
				},
			},
		}
	case *backend.SSHCredential:
		return &pb.Credential{
			CredentialUuid: c.GetUUID().String(),
			CredentialName: c.GetName(),
			Type:           pb.CredentialType_SSH_CREDENTIAL_TYPE,
			Details: &pb.Credential_Ssh{
				Ssh: &pb.SshDetails{
					Username:   c.Username,
					Host:       c.Host,
					Port:       int32(c.Port),
					PublicKey:  c.PublicKey,
					PrivateKey: c.PrivateKey,
					UseAgent:   c.UseAgent,
				},
			},
		}
	}
	return nil
}
