package task

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/optimiware/kodema/internal/control/pipeline"
	"github.com/optimiware/kodema/internal/fault"
)

// Task conversation bound. Metadata bounds and parsing live in
// pipeline.ParseTaskMetadata; the conversation is the admitted message history
// snapshot.
const maxConversationBytes = 64 << 10

// emptyConversationJSON is the present conversation snapshot of a task-bound
// admission with zero messages. It is the valid JSON empty array, the same
// encoding a non-empty conversation uses (a JSON array of messages), so a
// workload can parse /kodema/task.json. It is non-nil: presence is distinct
// from the nil task-free case, which materializes no file.
const emptyConversationJSON = "[]"

// parseMetadata validates and parses one opaque task metadata object through
// the shared pipeline.ParseTaskMetadata decoder.
func parseMetadata(raw []byte) (map[string]string, error) {
	return pipeline.ParseTaskMetadata(raw)
}

// marshalMetadata re-encodes stored metadata for a fresh admission. Metadata is
// never interpreted, so the canonical object bytes are the only requirement.
func marshalMetadata(metadata map[string]string) []byte {
	if len(metadata) == 0 {
		return nil
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil
	}
	return encoded
}

// conversationBytes builds the admitted conversation snapshot from the stored
// messages plus any additional messages committed in the same operation (such
// as a reply's answer and, if not already persisted in messages, the answered
// question). Every caller is a task-bound admission, so a zero-message combined
// conversation is the present JSON empty array (non-nil, "[]"), never nil: nil
// is reserved for a task-free execution, and a task-bound run with nil bytes
// materializes no /kodema/task.json while KODEMA_TASK is still set. The empty
// array keeps the file valid JSON and consistent with the non-empty
// message-array encoding.
func conversationBytes(messages []Message, additions ...*Message) ([]byte, error) {
	combined := append([]Message(nil), messages...)
	for _, addition := range additions {
		if addition != nil {
			combined = append(combined, *addition)
		}
	}
	if len(combined) == 0 {
		return []byte(emptyConversationJSON), nil
	}
	encoded, err := json.Marshal(combined)
	if err != nil {
		return nil, fault.Wrap(fault.InternalFailure, "task conversation cannot be encoded", err)
	}
	if len(encoded) > maxConversationBytes {
		return nil, fault.New(fault.InvalidInput, fmt.Sprintf("task conversation is %d bytes; at most %d are allowed", len(encoded), maxConversationBytes))
	}
	return encoded, nil
}

// questionMessage projects an outstanding question into its durable
// conversation message representation, preserving author, timestamp, body and
// durable references.
func questionMessage(question *Question) *Message {
	if question == nil || question.Body == "" {
		return nil
	}
	return &Message{
		Author:     question.AskedBy,
		At:         question.At,
		Body:       question.Body,
		References: cloneStrings(question.References),
	}
}

// insertMessageChronologically returns a copy of messages with addition
// inserted at its chronological position by timestamp (after any existing
// messages at or before addition.At), keeping legacy backfilled questions
// aligned with task.read/task.history ordering even when comments arrived
// while the task was waiting.
func insertMessageChronologically(messages []Message, addition *Message) []Message {
	if addition == nil {
		return messages
	}
	index := len(messages)
	for i, message := range messages {
		if message.At.After(addition.At) {
			index = i
			break
		}
	}
	out := make([]Message, 0, len(messages)+1)
	out = append(out, messages[:index]...)
	out = append(out, *addition)
	out = append(out, messages[index:]...)
	return out
}

// messagesContainQuestion reports whether the task's stored conversation
// messages already contain the durable message recorded for question.
func messagesContainQuestion(messages []Message, question *Question) bool {
	if question == nil {
		return false
	}
	for _, message := range messages {
		if message.Author == question.AskedBy &&
			message.At.Equal(question.At) &&
			message.Body == question.Body &&
			slices.Equal(message.References, question.References) {
			return true
		}
	}
	return false
}
