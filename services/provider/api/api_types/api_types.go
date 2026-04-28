package api_types

// NotifyReason is the semantic value of NotifyRequest.reason (uint32 on the wire).
type NotifyReason uint

const (
	NotifyReasonUnknown NotifyReason = iota
	NotifyReasonObcCreated
	NotifyReasonObcDeleted
)
