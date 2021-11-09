package main

import (
	"bytes"
	"fmt"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"errors"
	"time"
	"strings"

	git "github.com/go-git/go-git"
	plumbing "github.com/go-git/go-git/plumbing"
	git_http "github.com/go-git/go-git/plumbing/transport/http"
	plumbing_object "github.com/go-git/go-git/plumbing/object"
)

const (
	SuccessState string  = "Success"
	FailedState string = "Failed"
	RunningState string = "Running"
	DefaultRemoteName string = "origin"
)

//Commands
const (
	Clone string = "clone"
	Commit string = "commit"
	Pull string = "pull"
	Push string = "push"
	CreateBranch string = "create_branch"
	Checkout string = "checkout"
)

type GitCommandExecutionFinalStatus struct {
	GitOpExecutionState    string `json:"executionState"`
	Message string `json:"message"`
}

type ExecutionState struct {
	GitOpExecutionState string `json:"executionState"`
}

type CloneCommandConfiguration struct {
	Type string
	CommandType string
	Url         string
	RemoteName  string
	Path     string
	Provider    string
}

type CommitterSignature struct {
	Name string `json:"name"`
	Email string `json:"email"`
	Time time.Time `json:"time"`
}

type CommitDTO struct {
	Name string `json:"name"`
	Email string `json:"email"`
	Time time.Time `json:"time"`
	CommitHash string `json:"commitHash"`
	Message string `json:"message"`
}

type CommitCommandConfiguration struct {
	Type string `json:"type"`
	CommandType string `json:"commandType"`
	Path string `json:"path"`
	Message string `json:"message"`
	All bool `json:"all"`
	Branch string `json:"branch"`
	CommitterSignature CommitterSignature `json:"committer"`
}

type PushCommandConfiguration struct {
	Type string `json:"type"`
	CommandType string `json:"commandType"`
	Path string `json:"path"`
	RemoteName string `json:"remoteName"`
	Force bool `json:"force"`
}

type CreateBranchCommandConfiguration struct {
	Type string `json:"type"`
	CommandType string `json:"commandType"`
	Path string `json:"path"`
	NewBranchName string `json:"branchName"`
}

type CheckoutCommandConfiguration struct {
	Type string `json:"type"`
	CommandType string `json:"commandType"`
	Path string `json:"path"`
	BranchName string `json:"branchName"`
	Force bool `json:"force"`
}

type GitRepositoryDTO struct {
	RemoteName string `json:"remoteName"`
	DefaultBranch string `json:"defaultBranch"`
}


type BranchCommitsDTO struct {
	Commits  []CommitDTO `json:"commits"`
}

var gitCommand string
var gitCommandConfiguration string
var token string
var remoteEndpointUrl string
var projectIdStr string
var executionIdStr string
var gitUsername string
var gitToken string
var baseDir string
var userDir string
var repositoryId string

func main() {
	CheckArgs("commandType", "commandConfig", "hdfsUser", "tokenFile", "executionId", "projectId", "gitUsername",
		"gitToken", "repositoryId")
	commandType, commandConfig, hdfsUser, secretDir := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	executionIdStr, projectIdStr = os.Args[5], os.Args[6]
	gitUsername, gitToken, repositoryId = os.Args[7], os.Args[8], os.Args[9]
	
	fileBytes, err := ioutil.ReadFile(secretDir + "/token.jwt")
	token = string(fileBytes)

	if err != nil {
		CheckIfError(err)
	}
	baseDir = "/srv/hops/git"
	userDir = baseDir + "/" + hdfsUser

	updateStateToRunningOnRemote()
	switch commandType {
	case Clone:
		executeClone(commandConfig)
	case Commit:
		executeCommit(commandConfig)
	case Push:
		executePush(commandConfig)
	case CreateBranch:
		executeCreateBranch(commandConfig)
	case Checkout:
		executeCheckout(commandConfig)
	default:
		sendCommandFinalResultResponseToRemote("Unknown command type", FailedState)
	}
}

func executeCheckout(commandConfig string) {
	var checkoutCommandConfig CheckoutCommandConfiguration
	json.Unmarshal([]byte(commandConfig), &checkoutCommandConfig)
	directory := userDir + checkoutCommandConfig.Path
	r, err := git.PlainOpen(directory)
	CheckIfError(err)
	Info("Checking out to branch " + checkoutCommandConfig.BranchName)
	w, err := r.Worktree()
	CheckIfError(err)
	branch := fmt.Sprintf("refs/heads/%s", checkoutCommandConfig.BranchName)
	b := plumbing.ReferenceName(branch)
	Info("Reference is %s", b)
	err = w.Checkout(&git.CheckoutOptions{
		Branch: b,
		Force: checkoutCommandConfig.Force,
	})
	CheckIfError(err)
	sendCommandFinalResultResponseToRemote("Checked out to branch " + checkoutCommandConfig.BranchName, SuccessState)
}

func executeCreateBranch(commandConfig string) {
	var createBranchCommandConfig CreateBranchCommandConfiguration
	json.Unmarshal([]byte(commandConfig), &createBranchCommandConfig)
	branchName := createBranchCommandConfig.NewBranchName
	directory := userDir + createBranchCommandConfig.Path
	r, err := git.PlainOpen(directory)
	CheckIfError(err)
	Info("Creating new branch: git branch %s", branchName)
	headRef, err := r.Head()
	CheckIfError(err)
	branch := fmt.Sprintf("refs/heads/%s", branchName)
	b := plumbing.ReferenceName(branch)
	ref := plumbing.NewHashReference(b, headRef.Hash())
	// The created reference is saved in the storage.
	err = r.Storer.SetReference(ref)
	CheckIfError(err)
	sendCommandFinalResultResponseToRemote("New branch successfully created.", SuccessState)
}

func executePush(commandConfig string)  {
	var pushCommandConfig PushCommandConfiguration
	json.Unmarshal([]byte(commandConfig), &pushCommandConfig)
	directory := userDir + pushCommandConfig.Path
	// Open the git repo.
	r, err := git.PlainOpen(directory)
	CheckIfError(err)
	err = r.Push(&git.PushOptions{
		RemoteName: pushCommandConfig.RemoteName,
		Auth: &git_http.BasicAuth{
			Username: gitUsername,
			Password: gitToken,
		},
		Force: pushCommandConfig.Force,
	})
	CheckIfError(err)
	sendCommandFinalResultResponseToRemote("Push operation successful.", SuccessState)
}

func executeCommit(commandConfig string)  {
	var commitCommandConfig CommitCommandConfiguration
	json.Unmarshal([]byte(commandConfig), &commitCommandConfig)
	directory := userDir + commitCommandConfig.Path
	// Open the git repo.
	r, err := git.PlainOpen(directory)
	CheckIfError(err)
	w, err := r.Worktree()
	CheckIfError(err)

	// Check the status.
	Info("git status --porcelain")
	status, err := w.Status()
	CheckIfError(err)
	Info("Current Status is %s", status)
	if status.IsClean() {
		err := errors.New("Nothing to commit, working tree clean.")
		CheckIfError(err)
	}

	//add all
	err = w.AddWithOptions(&git.AddOptions{All: true})
	CheckIfError(err)

	//check new status
	Info("git status --porcelain")
	new_status, err := w.Status()
	CheckIfError(err)
	Info("Current Status is %s", new_status)

	commit, err := w.Commit(commitCommandConfig.Message, &git.CommitOptions{
		Author: &plumbing_object.Signature{
			Name:  commitCommandConfig.CommitterSignature.Name,
			Email: commitCommandConfig.CommitterSignature.Email,
			When:  time.Now(),
		},
	})
	CheckIfError(err)

	//Get the current HEAD to verify that all worked well.
	/*
	Info("Git show  -s")
	obj, err := r.CommitObject(commit)
	CheckIfError(err)
	Info("Commit object %s", obj)*/

	// We can verify if all worked well by trying to find the commit object later.
	sendCommandFinalResultResponseToRemote("Commit was successful. Current head is " + commit.String(), SuccessState)
}

func executeClone(commandConfig string)  {
	var cloneCommandConfig CloneCommandConfiguration
	json.Unmarshal([]byte(commandConfig), &cloneCommandConfig)

	url := cloneCommandConfig.Url
	Info("Command dir path is %s ", cloneCommandConfig.Path)
	directory := userDir + cloneCommandConfig.Path

	gitCloneOptions := &git.CloneOptions{
		URL:               url,
		RecurseSubmodules: git.DefaultSubmoduleRecursionDepth,
		Progress: os.Stdout,
	}

	gitCloneOptions.Auth = &git_http.BasicAuth {
		Username: gitUsername, // yes, this can be anything except an empty string
		Password: gitToken,
	}

	r, err := git.PlainClone(directory, false, gitCloneOptions)
	CheckIfError(err)

	// ... retrieving the branch being pointed by HEAD
	ref, err := r.Head()
	CheckIfError(err)
	
	branchName := strings.ReplaceAll(ref.Name().String(), "refs/heads/", "")
	Info("Branch name on remote is %s", branchName)
	// ... retrieving the commit object
	commit, err := r.CommitObject(ref.Hash())
	CheckIfError(err)
	commitHash := strings.TrimSpace(strings.ReplaceAll(commit.String(), "commit", ""))
	Info("Commit %s", commitHash)

	//update the repository
	updateRepositoryOnRemote(cloneCommandConfig, branchName)
	commits, err := getCommits(r)
	Info("Commits are %s", commits)
	if err != nil {
		Info("Error when getting commits %s", err)
		updateCommitsOnRemote(commits, branchName)
	}
	sendCommandFinalResultResponseToRemote("Repository successfully cloned", SuccessState)
}

func updateRepositoryOnRemote(configuration CloneCommandConfiguration, branchName string) {
	remoteEndpointUrl = "https://10.0.2.15:8181/hopsworks-api/api/project/"+projectIdStr+"/git/"+repositoryId+"/repository"
	body := &GitRepositoryDTO {
		RemoteName: DefaultRemoteName,
		DefaultBranch: branchName,
	}
	postBody := new(bytes.Buffer)
	json.NewEncoder(postBody).Encode(body)
	sendHttpReq(remoteEndpointUrl, postBody, http.MethodPut)
}

func sendCommandFinalResultResponseToRemote(message string, finalStatus string)  {
	remoteEndpointUrl = "https://10.0.2.15:8181/hopsworks-api/api/project/"+projectIdStr+"/git/"+repositoryId+ "/executions" + "/"+executionIdStr+"/finalStatus"

	body := &GitCommandExecutionFinalStatus {
		GitOpExecutionState:    finalStatus,
		Message: message,
	}
	postBody := new(bytes.Buffer)
	json.NewEncoder(postBody).Encode(body)
    sendHttpReq(remoteEndpointUrl, postBody, http.MethodPut)
}

func updateCommitsOnRemote(commits []CommitDTO, branchName string) {
	remoteEndpointUrl := "https://10.0.2.15:8181/hopsworks-api/api/project/"+projectIdStr+"/git/" + repositoryId	+ "/branch/"+ branchName
	postBody := new(bytes.Buffer)
	body := &BranchCommitsDTO{
		Commits: commits,
	}
	json.NewEncoder(postBody).Encode(body)
	sendHttpReq(remoteEndpointUrl, postBody, http.MethodPut)
}

func updateStateToRunningOnRemote() {
	remoteEndpointUrl = "https://10.0.2.15:8181/hopsworks-api/api/project/"+projectIdStr+"/git/"+repositoryId+"/executions" + "/"+executionIdStr+"/updateState"
	body := &ExecutionState{
		GitOpExecutionState:  RunningState,
	}
	postBody := new(bytes.Buffer)
	json.NewEncoder(postBody).Encode(body)
	sendHttpReq(remoteEndpointUrl, postBody, http.MethodPut)
}

func sendHttpReq(remoteUrl string, postBody *bytes.Buffer, httpMethod string) {
	Info("Sending to URL %s", remoteUrl)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	bearer := "Bearer " + token
	req, err := http.NewRequest(httpMethod, remoteUrl, postBody)
	req.Header.Set("Authorization", bearer)
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json;charset=UTF-8")

	client := &http.Client{}
	client.Transport = tr
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		for key, val := range via[0].Header {
			req.Header[key] = val
		}
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		CheckIfError(err)
	} else {
		defer resp.Body.Close()
		data, _ := ioutil.ReadAll(resp.Body)
		Info("Received data is %s", string(data))
	}
}


// CheckArgs should be used to ensure the right command line arguments are
// passed before executing the git command
func CheckArgs(arg ...string) {
	if len(os.Args) < len(arg)+1 {
		Warning("Usage: %s %s", os.Args[0], strings.Join(arg, " "))
		os.Exit(1)
	}
}

// CheckIfError should be used to naively panics if an error is not nil.
func CheckIfError(err error) {
	if err == nil {
		return
	}
	fmt.Printf("\x1b[31;1m%s\x1b[0m\n", fmt.Sprintf("error: %s", err))
	sendCommandFinalResultResponseToRemote(err.Error(), FailedState)
	os.Exit(1)
}

// Info should be used to describe the example commands that are about to run.
func Info(format string, args ...interface{}) {
	fmt.Printf("\x1b[34;1m%s\x1b[0m\n", fmt.Sprintf(format, args...))
}

// Warning should be used to display a warning
func Warning(format string, args ...interface{}) {
	fmt.Printf("\x1b[36;1m%s\x1b[0m\n", fmt.Sprintf(format, args...))
}

//test is worktree is clean
func isClean(w *git.Worktree) (isClean bool, err error) {
	status, err := w.Status()
	if err != nil {
		return false, err
	} else {
		return status.IsClean(), nil
	}
}

func getCommits(r *git.Repository) (commitsDTOs []CommitDTO, err error){
	// List the history of the repository
	Info("git log --oneline")
	commitIter, err := r.Log(&git.LogOptions{All: true})
	if err != nil {
		return commitsDTOs, err
	}
	count := 0
	for {
		commit, err := commitIter.Next()
		if err != nil || count >= 20 {
			break
		}
		commitDTO := CommitDTO{
			Name: commit.Committer.Name,
			Email: commit.Committer.Email,
			Time: commit.Committer.When,
			Message: commit.Message,
			CommitHash: commit.Hash.String(),
		}

		commitsDTOs = append(commitsDTOs, commitDTO)
		count++
	}
	return commitsDTOs, nil
}
