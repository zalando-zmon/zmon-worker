import opentracing

from opentracing import child_of, follows_from


def extract_tracing_span(carrier, use_follows_from=False):
    try:
        span_context = opentracing.tracer.extract(opentracing.Format.TEXT_MAP, carrier)

        references = [follows_from(span_context)] if use_follows_from else [child_of(span_context)]

        return opentracing.tracer.start_span(references=references)
    except Exception:
        return opentracing.tracer.start_span()


def inject_tracing_span(span, carrier):
    opentracing.tracer.inject(span.context, opentracing.Format.TEXT_MAP, carrier)
    return carrier
