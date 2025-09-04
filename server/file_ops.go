package server

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/config"
	"github.com/jgoldverg/grover/pb"
)

type FileService struct {
	pb.UnimplementedFileServiceServer
	storage backend.CredentialStorage
}

func NewFileService(serverConfig *config.ServerConfig) *FileService {
	storage, _ := backend.NewTomlCredentialStorage(serverConfig.CredentialsFile)
	return &FileService{
		UnimplementedFileServiceServer: pb.UnimplementedFileServiceServer{},
		storage:                        storage,
	}
}

func (fileService *FileService) List(ctx context.Context, in *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	if in.GetType() == pb.EndpointType_LOCAL_FS {
		fileSystemLister := backend.ListerFactory(fs.BackendType(in.GetType().String()), nil)
		listResult := fileSystemLister.List(in.GetPath())
		resp := pb.ListFilesResponse{Files: make([]*pb.FileInfo, 0)}
		for _, val := range listResult {
			resp.Files = append(resp.Files, &pb.FileInfo{
				Path: val.AbsPath,
				Size: val.Size,
				Id:   val.ID,
			})
		}
		return &resp, nil
	}
	var cred backend.Credential
	var getCredErr error
	if in.GetCredentialName() != "" {
		cred, getCredErr = fileService.storage.GetCredentialByName(in.GetCredentialName())
	}
	if in.GetCredentialUUID() != "" {
		id, err := uuid.Parse(in.GetCredentialUUID())
		if err != nil {
			return nil, err
		}
		cred, getCredErr = fileService.storage.GetCredentialByUUID(id)
	}
	if getCredErr != nil {
		return nil, getCredErr
	}

	fileSystemLister := backend.ListerFactory(fs.BackendType(in.GetType().String()), cred)
	listResult := fileSystemLister.List(in.GetPath())

	resp := pb.ListFilesResponse{Files: make([]*pb.FileInfo, 0)}
	for _, val := range listResult {
		resp.Files = append(resp.Files, &pb.FileInfo{
			Path: val.AbsPath,
			Size: val.Size,
			Id:   val.ID,
		})
	}

	return &resp, nil
}

func (fileService *FileService) Remove(ctx context.Context, in *pb.RemoveFileRequest) (*pb.RemoveFileResponse, error) {
	if in.GetType() == pb.EndpointType_LOCAL_FS {
		fileRmFactory := backend.RmFactory(fs.BackendType(in.GetType()), nil)
		deleted := fileRmFactory.Rm(in.GetPath())
		return &pb.RemoveFileResponse{Success: deleted}, nil
	}

	var cred backend.Credential
	var err error
	if in.GetCredentialName() != "" {
		cred, err = fileService.storage.GetCredentialByName(in.GetCredentialName())
		if err != nil {
			return nil, err
		}
	} else if in.GetCredentialUUID() != "" {
		id, err := uuid.Parse(in.GetCredentialUUID())
		if err != nil {
			return nil, err
		}
		cred, err = fileService.storage.GetCredentialByUUID(id)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("invalid credential name")
	}

	rmFactory := backend.RmFactory(fs.BackendType(in.GetType()), cred)
	deleted := rmFactory.Rm(in.GetPath())
	return &pb.RemoveFileResponse{Success: deleted}, nil
}
