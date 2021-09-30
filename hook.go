package hook

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type OpenTelemetryHook struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

func NewOpenTelemetryHook(tp trace.TracerProvider) *OpenTelemetryHook {
	propagator := propagation.NewCompositeTextMapPropagator(Metadata{}, propagation.Baggage{}, propagation.TraceContext{})
	otel.SetTracerProvider(tp)
	tracer := otel.Tracer("db")
	return &OpenTelemetryHook{
		tracer:     tracer,
		propagator: propagator,
	}
}

func (hook *OpenTelemetryHook) start(ctx context.Context, cmd redis.Cmder) (context.Context, trace.Span) {
	operation := fmt.Sprintf("%s", cmd.String())
	return hook.tracer.Start(ctx,
		operation,
		trace.WithSpanKind(trace.SpanKindClient),
	)
}

func (hook *OpenTelemetryHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return trace.ContextWithSpan(hook.start(ctx, cmd)), nil
}

func (hook *OpenTelemetryHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	var (
		span  = trace.SpanFromContext(ctx)
		tn    = time.Now()
		attrs = make([]attribute.KeyValue, 0)
	)

	attrs = append(attrs, attribute.Key("args").String(fmt.Sprintf("%v", cmd.Args())))
	attrs = append(attrs, attribute.Key("name").String(fmt.Sprintf("%v", cmd.Name())))
	attrs = append(attrs, attribute.Key("command").String(fmt.Sprintf("%v", cmd.String())))
	attrs = append(attrs, attribute.Key("redis.driver").String("go-redis"))

	if err := cmd.Err(); err != nil {
		span.RecordError(err)
	}

	span.SetAttributes(attrs...)
	span.End(trace.WithStackTrace(true), trace.WithTimestamp(tn))
	return nil
}

func (hook *OpenTelemetryHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (hook *OpenTelemetryHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return nil
}
