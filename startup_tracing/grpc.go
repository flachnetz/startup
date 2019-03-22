package startup_tracing

import (
	"context"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/modern-go/gls"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"google.golang.org/grpc"
)

type PreCheckFunc func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (context.Context, error)

func TracedInterceptor(checkFunc PreCheckFunc) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {

		meta, _ := metadata.FromIncomingContext(ctx)
		logrus.WithField("prefix", "traced-interceptor").Debugf("incoming md: %+v", meta)
		if checkFunc != nil {
			if checkedContext, err := checkFunc(ctx, req, info, handler); err != nil {
				return nil, err
			} else {
				ctx = checkedContext
			}
		}
		wireContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, MdCarrier(meta))
		if err != nil {
			logrus.WithField("prefix", "traced-interceptor").Errorf("error %s", err)
			return handler(ctx, req)
		}

		span := opentracing.GlobalTracer().StartSpan(info.FullMethod, ext.RPCServerOption(wireContext))
		defer span.Finish()
		ctxWithSpan := opentracing.ContextWithSpan(ctx, span)

		gls.WithGls(func() {
			WithSpan(span, func() {
				resp, err = handler(opentracing.ContextWithSpan(ctxWithSpan, span), req)
				s, _ := status.FromError(err)
				httpCode := runtime.HTTPStatusFromCode(s.Code())
				defer func() {
					grpc_opentracing.ClientAddContextTags(ctx, opentracing.Tags{"http.real_status_code": httpCode})
					ext.HTTPStatusCode.Set(span, uint16(httpCode))
				}()

			})
		})()

		return

	}
}

type MdCarrier metadata.MD

func (mdCarrier MdCarrier) ForeachKey(handler func(key, val string) error) error {
	md := (metadata.MD)(mdCarrier)

	for key, values := range md {
		if err := handler(key, values[0]); err != nil {
			return err
		}
	}

	return nil
}
