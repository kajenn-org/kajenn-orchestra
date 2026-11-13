# The kajenn-orchestra image: kajenn-orchestra on top of the kajenn image.
#
# The build context is dist/ holding the kajenn-orchestra wheel (see
# .dockerignore): the release job passes the wheel it published, the nightly
# job a wheel built from develop.
#
# KAJENN_IMAGE names the kajenn image, by digest in the CI, so the base is
# known exactly. The packages of the base win: the wheel is installed with
# their versions as constraints, and a requirement the base does not meet
# stops the build.
#
# /image-packages.txt lists every Python package of the image with its
# version: an image or a bundle built on top of this one reads it.
#
# The start command is the one of the kajenn image: `kajenn serve` on
# /app/config.py.
ARG KAJENN_IMAGE=ghcr.io/kajenn-org/kajenn:latest
FROM ${KAJENN_IMAGE}

USER root
COPY dist/*.whl /tmp/wheels/
RUN pip list --format=freeze > /tmp/base-packages.txt \
    && pip install --no-cache-dir -c /tmp/base-packages.txt /tmp/wheels/*.whl \
    && pip check \
    && rm -rf /tmp/wheels /tmp/base-packages.txt \
    && pip list --format=freeze > /image-packages.txt
USER kajenn
