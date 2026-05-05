# IBKR Client Portal API Gateway
#
# Ce container télécharge et lance le clientportal.gw d'IBKR.
# Il expose le port 5000 sur le réseau Docker interne.
#
# AUTHENTIFICATION (étape manuelle unique) :
#   1. Sur le VPS : ssh -L 5000:localhost:5000 user@vps
#   2. Dans le navigateur : https://localhost:5000
#   3. Se connecter avec les credentials IBKR
#   4. La session est maintenue par le keepalive du ibkr-broker (toutes les 55 s)
#
# VERSION : téléchargée depuis le site officiel IBKR (lien stable)

FROM eclipse-temurin:17-jre-alpine

WORKDIR /root/clientportal

# Télécharge le Client Portal Gateway depuis IBKR
# Note : le lien stable est maintenu par IBKR — vérifier la version en cas de changement
RUN apk add --no-cache bash curl unzip && \
    curl -fsSL "https://download2.interactivebrokers.com/portal/clientportal.gw.zip" -o portal.zip && \
    unzip -q portal.zip && \
    rm portal.zip && \
    chmod +x bin/run.sh

# Copie la configuration
COPY conf.yaml root/conf.yaml

EXPOSE 5000

# Lance le gateway en mode headless
CMD ["/bin/bash", "/root/clientportal/bin/run.sh", "root/conf.yaml"]
