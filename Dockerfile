FROM ghcr.io/dojoengine/dojo-base:vbase-1

ENTRYPOINT ["/tini", "--"]

ARG TARGETPLATFORM

LABEL description="Torii is an automatic indexer for Dojo." \
	authors="Dojo team" \
	source="https://github.com/dojoengine/torii" \
	documentation="https://book.dojoengine.org/"

COPY --from=artifacts --chmod=755 $TARGETPLATFORM/torii /usr/local/bin/
