FROM nginx:1.25-alpine

# Install envsubst for environment variable substitution
RUN apk add --no-cache gettext

# Remove default NGINX configuration that conflicts with our setup
RUN rm -f /etc/nginx/conf.d/default.conf

# Copy nginx configuration template
COPY nginx.conf /etc/nginx/nginx.conf.template

# Create entrypoint script for environment variable substitution
RUN echo '#!/bin/sh' > /docker-entrypoint.sh && \
    echo 'set -e' >> /docker-entrypoint.sh && \
    echo 'rm -f /etc/nginx/conf.d/default.conf' >> /docker-entrypoint.sh && \
    echo 'envsubst "\$NGINX_PORT \$NGINX_SERVER_NAME \$BACKEND_HOST \$BACKEND_PORT \$FRONTEND_HOST \$FRONTEND_PORT" < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf' >> /docker-entrypoint.sh && \
    echo 'exec nginx -g "daemon off;"' >> /docker-entrypoint.sh && \
    chmod +x /docker-entrypoint.sh

EXPOSE 80

STOPSIGNAL SIGQUIT

ENTRYPOINT ["/docker-entrypoint.sh"]