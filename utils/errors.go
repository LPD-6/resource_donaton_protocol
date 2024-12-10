package utils

import "errors"

var (
    ErrTaskAssignment   = errors.New("failed to assign task")
    ErrResultAggregation = errors.New("failed to aggregate results")
    ErrNodeRegistration  = errors.New("failed to register node")
    ErrNodeDisconnected = errors.New("node disconnected")
    TerminationMessage = "TERMINATE"
)