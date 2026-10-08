package tests

import "testing"

import "github.com/mohammednumaan/flux/internal/utils"

func TestGroupFromTags(t *testing.T) {
	tests := []struct {
		name string
		tags []string
		want string
	}{
		{
			name: "returns the backend group tag",
			tags: []string{"version=v1", "flux-backend-group=constrained"},
			want: "constrained",
		},
		{
			name: "returns unknown when the backend group tag is absent",
			tags: []string{"version=v1"},
			want: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.GroupFromTags(tt.tags); got != tt.want {
				t.Fatalf("GroupFromTags(%v) = %q, want %q", tt.tags, got, tt.want)
			}
		})
	}
}

func TestMaxRequestsFromTags(t *testing.T) {
	tests := []struct {
		name string
		tags []string
		want int64
	}{
		{
			name: "returns the max requests tag",
			tags: []string{"flux-max-requests=75"},
			want: 75,
		},
		{
			name: "returns zero when the tag is absent",
			tags: []string{"version=v1"},
			want: 0,
		},
		{
			name: "returns zero for an invalid value",
			tags: []string{"flux-max-requests=invalid"},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.MaxRequestsFromTags(tt.tags); got != tt.want {
				t.Fatalf("MaxRequestsFromTags(%v) = %d, want %d", tt.tags, got, tt.want)
			}
		})
	}
}
