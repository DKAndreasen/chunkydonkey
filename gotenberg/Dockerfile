FROM gotenberg/gotenberg:8

USER root

# Bring fonts into the container
COPY gotenberg/fonts/ /usr/share/fonts/truetype/custom/

# Rebuild font cache
RUN fc-cache -f -v

USER gotenberg