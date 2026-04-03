pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
  }

  triggers {
    githubPush()
  }

  environment {
    REPO_DIR = '/root/turnstile-auth-provider-src'
    COMPOSE_FILE = '/root/turnstile-auth-provider-deploy/docker-compose.yml'
    REPO_URL = 'https://github.com/ckkkf/Turnstile-Auth-Provider.git'
    REPO_BRANCH = 'master'
  }

  stages {
    stage('Checkout') {
      steps {
        sh '''
          set -eux
          if [ ! -d "$REPO_DIR/.git" ]; then
            git clone --branch "$REPO_BRANCH" --single-branch "$REPO_URL" "$REPO_DIR"
          fi
          cd "$REPO_DIR"
          git fetch origin "$REPO_BRANCH" --prune
          git checkout -B "$REPO_BRANCH" "origin/$REPO_BRANCH"
          git reset --hard "origin/$REPO_BRANCH"
        '''
      }
    }

    stage('Build and Deploy') {
      steps {
        sh '''
          set -eux
          mkdir -p /root/turnstile-auth-provider-data
          /usr/bin/docker-compose -f "$COMPOSE_FILE" build --pull
          /usr/bin/docker-compose -f "$COMPOSE_FILE" up -d
        '''
      }
    }

    stage('Health Check') {
      steps {
        sh '''
          set -eux
          for i in $(seq 1 24); do
            if curl -fsS http://127.0.0.1:5072/ >/tmp/turnstile-auth-provider-health.html; then
              cat /tmp/turnstile-auth-provider-health.html
              exit 0
            fi
            sleep 5
          done
          docker ps
          /usr/bin/docker-compose -f "$COMPOSE_FILE" logs --tail=200
          exit 1
        '''
      }
    }
  }

  post {
    success {
      echo 'turnstile-auth-provider deployed successfully'
    }
    failure {
      echo 'turnstile-auth-provider deployment failed'
    }
  }
}
