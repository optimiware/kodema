package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	maxAuthTransactions    = 4096
	maxSocialTransactions  = 4096
	maxGitHubSetupStates   = 64
	maxAuthCodes           = 4096
	maxRefreshTokens       = 8192
	maxDeviceSessions      = 2048
	maxDynamicClients      = 1024
	maxRevokedJTIs         = 8192
	browserFlowLifetime    = 10 * time.Minute
	unambiguousUserCharset = "BCDFGHJKLMNPQRSTVWXZ"
)

var (
	errInvalidGrant = errors.New("invalid_grant")
	errExpiredToken = errors.New("expired_token")
	errAuthPending  = errors.New("authorization_pending")
	errAccessDenied = errors.New("access_denied")
)

type txKind int

const (
	txAuthorize txKind = iota + 1
	txDevice
)

type authTx struct {
	ID            string
	Kind          txKind
	ClientID      string
	RedirectURI   string
	State         string
	Nonce         string
	Scope         string
	CodeChallenge string
	LoginHint     string
	DeviceHash    [32]byte
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

type socialTx struct {
	Provider      string
	AuthTxID      string
	BrowserID     string
	CodeVerifier  string
	UpstreamNonce string
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

type githubSetupTx struct {
	AuthTxID  string
	BrowserID string
	CreatedAt time.Time
	ExpiresAt time.Time
}

type authCodeEntry struct {
	ClientID      string
	RedirectURI   string
	CodeChallenge string
	Nonce         string
	Scope         string
	User          UserIdentity
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

type refreshEntry struct {
	TokenHash [32]byte
	ClientID  string
	Scope     string
	User      UserIdentity
	CreatedAt time.Time
	ExpiresAt time.Time
}

type deviceStatus int

const (
	devicePending deviceStatus = iota + 1
	deviceApproved
	deviceDenied
)

type deviceEntry struct {
	DeviceHash         [32]byte
	NormalizedUserCode string
	DisplayUserCode    string
	ClientID           string
	Scope              string
	Status             deviceStatus
	User               UserIdentity
	CreatedAt          time.Time
	ExpiresAt          time.Time
}

type registeredClient struct {
	ClientID     string
	RedirectURIs []string
	GrantTypes   []string
}

type authStore struct {
	mu          sync.Mutex
	authTxs     map[string]*authTx
	socialTxs   map[[32]byte]*socialTx
	ghSetupTxs  map[[32]byte]*githubSetupTx
	authCodes   map[[32]byte]*authCodeEntry
	refresh     map[string]*refreshEntry
	devices     map[[32]byte]*deviceEntry
	userCodes   map[string][32]byte
	clients     map[string]*registeredClient
	clientOrder []string
	revokedJTIs map[string]time.Time
}

func newAuthStore() *authStore {
	return &authStore{
		authTxs:     make(map[string]*authTx),
		socialTxs:   make(map[[32]byte]*socialTx),
		ghSetupTxs:  make(map[[32]byte]*githubSetupTx),
		authCodes:   make(map[[32]byte]*authCodeEntry),
		refresh:     make(map[string]*refreshEntry),
		devices:     make(map[[32]byte]*deviceEntry),
		userCodes:   make(map[string][32]byte),
		clients:     make(map[string]*registeredClient),
		revokedJTIs: make(map[string]time.Time),
	}
}

func secretDigest(value string) [32]byte {
	return sha256.Sum256([]byte(value))
}

func randomToken(random io.Reader, numBytes int) (string, error) {
	buf := make([]byte, numBytes)
	if _, err := io.ReadFull(random, buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// randomUserCode generates an 8-character XXXX-XXXX code from RFC 8628's
// unambiguous consonant alphabet without modulo bias.
func randomUserCode(random io.Reader) (string, error) {
	const codeLen = 8
	const alphabetLen = len(unambiguousUserCharset) // 20
	const maxUnbiased = byte(240)                   // 12 * 20
	chars := make([]byte, 0, codeLen+1)
	var one [1]byte
	for len(chars) < codeLen+1 {
		if len(chars) == 4 {
			chars = append(chars, '-')
			continue
		}
		if _, err := io.ReadFull(random, one[:]); err != nil {
			return "", err
		}
		if one[0] >= maxUnbiased {
			continue
		}
		chars = append(chars, unambiguousUserCharset[int(one[0])%alphabetLen])
	}
	return string(chars), nil
}

func normalizeUserCode(raw string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(strings.TrimSpace(raw)) {
		if r == '-' || r == ' ' || r == '\t' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func pruneAndEvict[K comparable, V any](
	entries map[K]V,
	maxEntries int,
	now time.Time,
	expiresAt func(V) time.Time,
	evictAt func(V) time.Time,
	onDelete func(V),
) {
	for k, v := range entries {
		if !now.Before(expiresAt(v)) {
			if onDelete != nil {
				onDelete(v)
			}
			delete(entries, k)
		}
	}
	if len(entries) < maxEntries {
		return
	}
	var oldestKey K
	var oldestSet bool
	var oldestTime time.Time
	for k, v := range entries {
		t := evictAt(v)
		if !oldestSet || t.Before(oldestTime) {
			oldestKey = k
			oldestTime = t
			oldestSet = true
		}
	}
	if oldestSet {
		if onDelete != nil {
			onDelete(entries[oldestKey])
		}
		delete(entries, oldestKey)
	}
}

func (store *authStore) putAuthTx(tx authTx, now time.Time) {
	store.mu.Lock()
	defer store.mu.Unlock()
	pruneAndEvict(store.authTxs, maxAuthTransactions, now,
		func(v *authTx) time.Time { return v.ExpiresAt },
		func(v *authTx) time.Time { return v.CreatedAt },
		nil)
	copied := tx
	store.authTxs[tx.ID] = &copied
}

func (store *authStore) getAuthTx(id string, now time.Time) (authTx, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.authTxs[id]
	if !ok {
		return authTx{}, false
	}
	if !now.Before(entry.ExpiresAt) {
		delete(store.authTxs, id)
		return authTx{}, false
	}
	return *entry, true
}

func (store *authStore) consumeAuthTx(id string, now time.Time) (authTx, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.authTxs[id]
	if !ok {
		return authTx{}, false
	}
	delete(store.authTxs, id)
	if !now.Before(entry.ExpiresAt) {
		return authTx{}, false
	}
	return *entry, true
}

func (store *authStore) putSocialTx(rawState string, tx socialTx, now time.Time) {
	hash := secretDigest(rawState)
	store.mu.Lock()
	defer store.mu.Unlock()
	pruneAndEvict(store.socialTxs, maxSocialTransactions, now,
		func(v *socialTx) time.Time { return v.ExpiresAt },
		func(v *socialTx) time.Time { return v.CreatedAt },
		nil)
	copied := tx
	store.socialTxs[hash] = &copied
}

func (store *authStore) consumeSocialTx(rawState string, provider string, now time.Time) (socialTx, bool) {
	hash := secretDigest(rawState)
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.socialTxs[hash]
	if !ok {
		return socialTx{}, false
	}
	delete(store.socialTxs, hash)
	if !now.Before(entry.ExpiresAt) || entry.Provider != provider {
		return socialTx{}, false
	}
	return *entry, true
}

func (store *authStore) putGitHubSetupTx(rawState, authTxID, browserID string, now time.Time) {
	hash := secretDigest(rawState)
	store.mu.Lock()
	defer store.mu.Unlock()
	pruneAndEvict(store.ghSetupTxs, maxGitHubSetupStates, now,
		func(v *githubSetupTx) time.Time { return v.ExpiresAt },
		func(v *githubSetupTx) time.Time { return v.CreatedAt },
		nil)
	store.ghSetupTxs[hash] = &githubSetupTx{
		AuthTxID:  authTxID,
		BrowserID: browserID,
		CreatedAt: now,
		ExpiresAt: now.Add(browserFlowLifetime),
	}
}

func (store *authStore) consumeGitHubSetupTx(rawState string, now time.Time) (githubSetupTx, bool) {
	hash := secretDigest(rawState)
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.ghSetupTxs[hash]
	if !ok {
		return githubSetupTx{}, false
	}
	delete(store.ghSetupTxs, hash)
	if !now.Before(entry.ExpiresAt) {
		return githubSetupTx{}, false
	}
	return *entry, true
}

func (store *authStore) putAuthCode(rawCode string, entry authCodeEntry, now time.Time) {
	hash := secretDigest(rawCode)
	store.mu.Lock()
	defer store.mu.Unlock()
	pruneAndEvict(store.authCodes, maxAuthCodes, now,
		func(v *authCodeEntry) time.Time { return v.ExpiresAt },
		func(v *authCodeEntry) time.Time { return v.CreatedAt },
		nil)
	copied := entry
	store.authCodes[hash] = &copied
}

func (store *authStore) consumeAuthCode(rawCode string, now time.Time) (authCodeEntry, error) {
	hash := secretDigest(rawCode)
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.authCodes[hash]
	if !ok {
		return authCodeEntry{}, errInvalidGrant
	}
	delete(store.authCodes, hash)
	if !now.Before(entry.ExpiresAt) {
		return authCodeEntry{}, errInvalidGrant
	}
	return *entry, nil
}

// refreshFamily returns the family ID that prefixes a "<family>.<secret>"
// refresh token. The store keeps one entry per family, holding the digest of
// the family's current token, so rotation never adds an entry and any other
// token of a known family is a replay.
func refreshFamily(rawToken string) string {
	family, _, _ := strings.Cut(rawToken, ".")
	return family
}

func (store *authStore) issueRefreshToken(rawToken, clientID, scope string, user UserIdentity, ttl time.Duration, now time.Time) {
	store.mu.Lock()
	defer store.mu.Unlock()
	pruneAndEvict(store.refresh, maxRefreshTokens, now,
		func(v *refreshEntry) time.Time { return v.ExpiresAt },
		func(v *refreshEntry) time.Time { return v.CreatedAt },
		nil)
	store.refresh[refreshFamily(rawToken)] = &refreshEntry{
		TokenHash: secretDigest(rawToken),
		ClientID:  clientID,
		Scope:     scope,
		User:      user,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
}

// rotateRefreshToken atomically replaces rawOldToken with rawNewToken, which
// must carry the same family prefix, revoking the family if a previously
// rotated token is replayed.
func (store *authStore) rotateRefreshToken(rawOldToken, rawNewToken, clientID string, ttl time.Duration, now time.Time) (refreshEntry, error) {
	family := refreshFamily(rawOldToken)
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.refresh[family]
	if !ok {
		return refreshEntry{}, errInvalidGrant
	}
	if !now.Before(entry.ExpiresAt) {
		delete(store.refresh, family)
		return refreshEntry{}, errInvalidGrant
	}
	if !constantTimeEqualStrings(entry.ClientID, clientID) {
		return refreshEntry{}, errInvalidGrant
	}
	if entry.TokenHash != secretDigest(rawOldToken) {
		// Replay of a rotated refresh token compromises the family; revoke it.
		delete(store.refresh, family)
		return refreshEntry{}, errInvalidGrant
	}
	entry.TokenHash = secretDigest(rawNewToken)
	entry.CreatedAt = now
	entry.ExpiresAt = now.Add(ttl)
	return *entry, nil
}

func (store *authStore) revokeRefreshToken(rawToken, clientID string) bool {
	family := refreshFamily(rawToken)
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.refresh[family]
	if !ok {
		return false
	}
	if !constantTimeEqualStrings(entry.ClientID, clientID) {
		return false
	}
	delete(store.refresh, family)
	return true
}

func (store *authStore) revokeJTI(jti string, expiresAt time.Time, now time.Time) {
	if jti == "" {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	identity := func(exp time.Time) time.Time { return exp }
	pruneAndEvict(store.revokedJTIs, maxRevokedJTIs, now, identity, identity, nil)
	store.revokedJTIs[jti] = expiresAt
}

func (store *authStore) isJTIRevoked(jti string, now time.Time) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	exp, ok := store.revokedJTIs[jti]
	if !ok {
		return false
	}
	if !now.Before(exp) {
		delete(store.revokedJTIs, jti)
		return false
	}
	return true
}

func (store *authStore) createDeviceSession(random io.Reader, clientID, scope string, ttl time.Duration, now time.Time) (string, string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	pruneAndEvict(store.devices, maxDeviceSessions, now,
		func(v *deviceEntry) time.Time { return v.ExpiresAt },
		func(v *deviceEntry) time.Time { return v.CreatedAt },
		func(v *deviceEntry) {
			if v != nil {
				delete(store.userCodes, v.NormalizedUserCode)
			}
		})

	rawDeviceCode, err := randomToken(random, 32)
	if err != nil {
		return "", "", err
	}
	devHash := secretDigest(rawDeviceCode)

	var displayCode, normCode string
	for attempts := 0; attempts < 16; attempts++ {
		candidate, err := randomUserCode(random)
		if err != nil {
			return "", "", err
		}
		norm := normalizeUserCode(candidate)
		if _, exists := store.userCodes[norm]; !exists {
			displayCode = candidate
			normCode = norm
			break
		}
	}
	if normCode == "" {
		return "", "", errors.New("could not allocate unique device user code")
	}

	store.devices[devHash] = &deviceEntry{
		DeviceHash:         devHash,
		NormalizedUserCode: normCode,
		DisplayUserCode:    displayCode,
		ClientID:           clientID,
		Scope:              scope,
		Status:             devicePending,
		CreatedAt:          now,
		ExpiresAt:          now.Add(ttl),
	}
	store.userCodes[normCode] = devHash
	return rawDeviceCode, displayCode, nil
}

func (store *authStore) lookupPendingDeviceByUserCode(userCode string, now time.Time) (deviceEntry, bool) {
	norm := normalizeUserCode(userCode)
	if norm == "" {
		return deviceEntry{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	devHash, ok := store.userCodes[norm]
	if !ok {
		return deviceEntry{}, false
	}
	entry, ok := store.devices[devHash]
	if !ok || !now.Before(entry.ExpiresAt) {
		delete(store.userCodes, norm)
		delete(store.devices, devHash)
		return deviceEntry{}, false
	}
	if entry.Status != devicePending {
		return deviceEntry{}, false
	}
	return *entry, true
}

func (store *authStore) resolveDeviceSession(devHash [32]byte, status deviceStatus, user UserIdentity, now time.Time) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.devices[devHash]
	if !ok || !now.Before(entry.ExpiresAt) {
		if ok {
			delete(store.userCodes, entry.NormalizedUserCode)
			delete(store.devices, devHash)
		}
		return false
	}
	if entry.Status != devicePending {
		return false
	}
	entry.Status = status
	entry.User = user
	delete(store.userCodes, entry.NormalizedUserCode)
	return true
}

func (store *authStore) pollDeviceSession(rawDeviceCode, clientID string, now time.Time) (deviceEntry, error) {
	devHash := secretDigest(rawDeviceCode)
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.devices[devHash]
	if !ok {
		return deviceEntry{}, errInvalidGrant
	}
	if !constantTimeEqualStrings(entry.ClientID, clientID) {
		return deviceEntry{}, errInvalidGrant
	}
	if !now.Before(entry.ExpiresAt) {
		delete(store.userCodes, entry.NormalizedUserCode)
		delete(store.devices, devHash)
		return deviceEntry{}, errExpiredToken
	}
	switch entry.Status {
	case devicePending:
		return deviceEntry{}, errAuthPending
	case deviceDenied:
		delete(store.userCodes, entry.NormalizedUserCode)
		delete(store.devices, devHash)
		return deviceEntry{}, errAccessDenied
	case deviceApproved:
		copied := *entry
		delete(store.userCodes, entry.NormalizedUserCode)
		delete(store.devices, devHash)
		return copied, nil
	default:
		return deviceEntry{}, errInvalidGrant
	}
}

func (store *authStore) registerClient(client registeredClient) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.clients) >= maxDynamicClients && len(store.clientOrder) > 0 {
		oldest := store.clientOrder[0]
		store.clientOrder = store.clientOrder[1:]
		delete(store.clients, oldest)
	}
	copied := client
	copied.RedirectURIs = append([]string(nil), client.RedirectURIs...)
	copied.GrantTypes = append([]string(nil), client.GrantTypes...)
	store.clients[client.ClientID] = &copied
	store.clientOrder = append(store.clientOrder, client.ClientID)
}

func (store *authStore) lookupRegisteredClient(clientID string) (registeredClient, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	entry, ok := store.clients[clientID]
	if !ok {
		return registeredClient{}, false
	}
	copied := *entry
	copied.RedirectURIs = append([]string(nil), entry.RedirectURIs...)
	copied.GrantTypes = append([]string(nil), entry.GrantTypes...)
	return copied, true
}
