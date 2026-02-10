import { trace } from '@opentelemetry/api';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { SEMRESATTRS_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

function resolveTracesEndpointUrl(): string | undefined {
  const direct = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;
  if (direct) return direct;

  const base = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  if (!base) return undefined;

  const normalizedBase = base.replace(/\/$/, '');
  return `${normalizedBase}/v1/traces`;
}

const serviceName = process.env.OTEL_SERVICE_NAME || 'twin-garmin-etl';
const tracesUrl = resolveTracesEndpointUrl();

const traceExporter = tracesUrl ? new OTLPTraceExporter({ url: tracesUrl }) : new OTLPTraceExporter();
const provider = new NodeTracerProvider({
  resource: resourceFromAttributes({
    [SEMRESATTRS_SERVICE_NAME]: serviceName,
  }),
  spanProcessors: [new SimpleSpanProcessor(traceExporter)],
});

provider.register();

export const tracer = trace.getTracer(serviceName);

export async function shutdownTracing(): Promise<void> {
  await provider.shutdown();
}
