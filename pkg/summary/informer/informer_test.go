package informer

import (
	"context"
	"testing"
	"time"

	"github.com/rancher/wrangler/v3/pkg/summary"
	"github.com/rancher/wrangler/v3/pkg/summary/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

var _ client.ExtendedInterface = (*mockClient)(nil)

type mockClient struct {
	listCalled  chan struct{}
	watchCalled chan struct{}
}

func (m *mockClient) Resource(resource schema.GroupVersionResource) client.NamespaceableResourceInterface {
	return m
}

func (m *mockClient) ResourceWithOptions(resource schema.GroupVersionResource, opts *client.Options) client.NamespaceableResourceInterface {
	return m
}

func (m *mockClient) Namespace(string) client.ResourceInterface {
	return m
}

func (m *mockClient) List(ctx context.Context, opts metav1.ListOptions) (*summary.SummarizedObjectList, error) {
	if m.listCalled != nil {
		m.listCalled <- struct{}{}
	}
	return &summary.SummarizedObjectList{
		ListMeta: metav1.ListMeta{
			ResourceVersion: "1",
		},
	}, nil
}

func (m *mockClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	if m.watchCalled != nil {
		m.watchCalled <- struct{}{}
	}
	return watch.NewFake(), nil
}

type mockClientUnsupported struct {
	mockClient
}

func (m *mockClientUnsupported) IsWatchListSemanticsUnSupported() bool {
	return true
}

type mockClientSupported struct {
	mockClient
}

func (m *mockClientSupported) IsWatchListSemanticsUnSupported() bool {
	return false
}

func TestNewFilteredSummaryInformer_WatchListSupport(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "test", Version: "v1", Resource: "tests"}
	namespace := "default"

	tests := []struct {
		name        string
		client      client.Interface
		expectList  bool
		expectWatch bool
	}{
		{
			name: "Client supporting watchlist (default)",
			client: &mockClient{
				listCalled:  make(chan struct{}, 1),
				watchCalled: make(chan struct{}, 1),
			},
			expectList: false,
		},
		{
			name: "Client explicitly supporting watchlist",
			client: &mockClientSupported{
				mockClient: mockClient{
					listCalled:  make(chan struct{}, 1),
					watchCalled: make(chan struct{}, 1),
				},
			},
			expectList: false,
		},
		{
			name: "Client explicitly NOT supporting watchlist",
			client: &mockClientUnsupported{
				mockClient: mockClient{
					listCalled:  make(chan struct{}, 1),
					watchCalled: make(chan struct{}, 1),
				},
			},
			expectList: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			informer := NewFilteredSummaryInformer(tt.client, gvr, namespace, 0, cache.Indexers{}, nil)
			stopCh := make(chan struct{})
			defer close(stopCh)

			go informer.Informer().Run(stopCh)

			// Wait for either List or Watch to be called
			var mc *mockClient
			switch c := tt.client.(type) {
			case *mockClient:
				mc = c
			case *mockClientSupported:
				mc = &c.mockClient
			case *mockClientUnsupported:
				mc = &c.mockClient
			}

			time.Sleep(100 * time.Millisecond)
			listCalled := false
			watchCalled := false

			select {
			case <-time.After(100 * time.Millisecond):
			case <-mc.listCalled:
				listCalled = true
			}

			select {
			case <-time.After(100 * time.Millisecond):
			case <-mc.watchCalled:
				watchCalled = true
			}

			if tt.expectList && !listCalled {
				t.Fatal("Expected list call but didn't get it")
			}

			if !tt.expectList && listCalled {
				t.Fatal("Expected NO list call")
			}

			if !watchCalled {
				t.Fatal("Expected watch call but didn't get it")
			}
		})
	}
}

// blockingClient blocks List or Watch until the informer's context is cancelled.
// IsWatchListSemanticsUnSupported returns true so the traditional List→Watch path
// is exercised, making both ListWithContextFunc and WatchFuncWithContext reachable independently.
type blockingClient struct {
	listCtxCh  chan context.Context
	watchCtxCh chan context.Context
}

var _ client.ExtendedInterface = (*blockingClient)(nil)

func (m *blockingClient) IsWatchListSemanticsUnSupported() bool { return true }
func (m *blockingClient) Resource(_ schema.GroupVersionResource) client.NamespaceableResourceInterface {
	return m
}
func (m *blockingClient) ResourceWithOptions(_ schema.GroupVersionResource, _ *client.Options) client.NamespaceableResourceInterface {
	return m
}
func (m *blockingClient) Namespace(string) client.ResourceInterface { return m }

func (m *blockingClient) List(ctx context.Context, _ metav1.ListOptions) (*summary.SummarizedObjectList, error) {
	if m.listCtxCh != nil {
		select {
		case m.listCtxCh <- ctx:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &summary.SummarizedObjectList{ListMeta: metav1.ListMeta{ResourceVersion: "1"}}, nil
}

func (m *blockingClient) Watch(ctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	if m.watchCtxCh != nil {
		select {
		case m.watchCtxCh <- ctx:
		default:
		}
		fw := watch.NewFake()
		go func() { <-ctx.Done(); fw.Stop() }()
		return fw, nil
	}
	return watch.NewFake(), nil
}

func TestNewFilteredSummaryInformer_ListUsesRunContext(t *testing.T) {
	listCtxCh := make(chan context.Context, 1)
	mc := &blockingClient{listCtxCh: listCtxCh}

	gvr := schema.GroupVersionResource{Group: "test", Version: "v1", Resource: "tests"}
	inf := NewFilteredSummaryInformer(mc, gvr, metav1.NamespaceAll, 0, cache.Indexers{}, nil)
	stopCh := make(chan struct{})
	go inf.Informer().Run(stopCh)

	var listCtx context.Context
	select {
	case listCtx = <-listCtxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("List was not called within timeout")
	}

	select {
	case <-listCtx.Done():
		t.Fatal("List context should not be cancelled before informer stops")
	default:
	}

	close(stopCh)

	select {
	case <-listCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("List context was not cancelled after informer stopped")
	}
}

func TestNewFilteredSummaryInformer_WatchUsesRunContext(t *testing.T) {
	watchCtxCh := make(chan context.Context, 1)
	mc := &blockingClient{watchCtxCh: watchCtxCh}

	gvr := schema.GroupVersionResource{Group: "test", Version: "v1", Resource: "tests"}
	inf := NewFilteredSummaryInformer(mc, gvr, metav1.NamespaceAll, 0, cache.Indexers{}, nil)
	stopCh := make(chan struct{})
	go inf.Informer().Run(stopCh)

	var watchCtx context.Context
	select {
	case watchCtx = <-watchCtxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Watch was not called within timeout")
	}

	select {
	case <-watchCtx.Done():
		t.Fatal("Watch context should not be cancelled before informer stops")
	default:
	}

	close(stopCh)

	select {
	case <-watchCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Watch context was not cancelled after informer stopped")
	}
}

func TestNewFilteredSummaryInformerWithOptions_ListUsesRunContext(t *testing.T) {
	listCtxCh := make(chan context.Context, 1)
	mc := &blockingClient{listCtxCh: listCtxCh}

	gvr := schema.GroupVersionResource{Group: "test", Version: "v1", Resource: "tests"}
	inf := NewFilteredSummaryInformerWithOptions(mc, gvr, nil, metav1.NamespaceAll, 0, cache.Indexers{}, nil)
	stopCh := make(chan struct{})
	go inf.Informer().Run(stopCh)

	var listCtx context.Context
	select {
	case listCtx = <-listCtxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("List was not called within timeout")
	}

	select {
	case <-listCtx.Done():
		t.Fatal("List context should not be cancelled before informer stops")
	default:
	}

	close(stopCh)

	select {
	case <-listCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("List context was not cancelled after informer stopped")
	}
}

func TestNewFilteredSummaryInformerWithOptions_WatchUsesRunContext(t *testing.T) {
	watchCtxCh := make(chan context.Context, 1)
	mc := &blockingClient{watchCtxCh: watchCtxCh}

	gvr := schema.GroupVersionResource{Group: "test", Version: "v1", Resource: "tests"}
	inf := NewFilteredSummaryInformerWithOptions(mc, gvr, nil, metav1.NamespaceAll, 0, cache.Indexers{}, nil)
	stopCh := make(chan struct{})
	go inf.Informer().Run(stopCh)

	var watchCtx context.Context
	select {
	case watchCtx = <-watchCtxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Watch was not called within timeout")
	}

	select {
	case <-watchCtx.Done():
		t.Fatal("Watch context should not be cancelled before informer stops")
	default:
	}

	close(stopCh)

	select {
	case <-watchCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Watch context was not cancelled after informer stopped")
	}
}

func TestNewFilteredSummaryInformerWithOptions_WatchListSupport(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "test", Version: "v1", Resource: "tests"}
	namespace := "default"

	tests := []struct {
		name        string
		client      client.ExtendedInterface
		expectList  bool
		expectWatch bool
	}{
		{
			name: "Client supporting watchlist (default)",
			client: &mockClient{
				listCalled:  make(chan struct{}, 1),
				watchCalled: make(chan struct{}, 1),
			},
			expectList: false,
		},
		{
			name: "Client explicitly supporting watchlist",
			client: &mockClientSupported{
				mockClient: mockClient{
					listCalled:  make(chan struct{}, 1),
					watchCalled: make(chan struct{}, 1),
				},
			},
			expectList: false,
		},
		{
			name: "Client explicitly NOT supporting watchlist",
			client: &mockClientUnsupported{
				mockClient: mockClient{
					listCalled:  make(chan struct{}, 1),
					watchCalled: make(chan struct{}, 1),
				},
			},
			expectList: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			informer := NewFilteredSummaryInformerWithOptions(tt.client, gvr, nil, namespace, 0, cache.Indexers{}, nil)
			stopCh := make(chan struct{})
			defer close(stopCh)

			go informer.Informer().Run(stopCh)

			// Wait for either List or Watch to be called
			var mc *mockClient
			switch c := tt.client.(type) {
			case *mockClient:
				mc = c
			case *mockClientSupported:
				mc = &c.mockClient
			case *mockClientUnsupported:
				mc = &c.mockClient
			}

			time.Sleep(100 * time.Millisecond)
			listCalled := false
			watchCalled := false

			select {
			case <-time.After(100 * time.Millisecond):
			case <-mc.listCalled:
				listCalled = true
			}

			select {
			case <-time.After(100 * time.Millisecond):
			case <-mc.watchCalled:
				watchCalled = true
			}

			if tt.expectList && !listCalled {
				t.Fatal("Expected list call but didn't get it")
			}

			if !tt.expectList && listCalled {
				t.Fatal("Expected NO list call")
			}

			if !watchCalled {
				t.Fatal("Expected watch call but didn't get it")
			}
		})
	}
}
