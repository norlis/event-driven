package health

import (
	"encoding/json"
	"net/http"
	"os"
	"time"
)

type Status struct {
	StartedAt time.Time `json:"startedAt"`
	Hostname  string    `json:"hostname"`
	Version   string    `json:"version"`
}

func NewStatus(version string) *Status {
	hostname, err := os.Hostname()
	if hostname == "" || err != nil {
		hostname = "localhost"
	}

	return &Status{
		StartedAt: time.Now().UTC(),
		Hostname:  hostname,
		Version:   version,
	}
}

func (u *Status) Uptime() string {

	return time.Since(u.StartedAt).String()
}

func (u *Status) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	results := struct {
		Uptime   string `json:"uptime"`
		Hostname string `json:"hostname"`
		Version  string `json:"version"`
	}{
		Uptime:   u.Uptime(),
		Hostname: u.Hostname,
		Version:  u.Version,
	}
	_ = json.NewEncoder(w).Encode(results)

}
