pipeline {
    agent any

    stages {
	    stage ('checkout') {
		    checkout scm
	    }

	    stage ('build') {
            def img = docker.build 'pg-db-tools:snapshot'

            stage ('test') {
                img.inside ("-v /etc/passwd:/etc/passwd -v /etc/group:/etc/group") {
                    sh script: """
virtualenv -p python3 venv
. venv/bin/activate
pip install .
pip install unittest-xml-reporting pycodestyle
rm -rf results
mkdir results
pycodestyle src > results/pycodestyle.log || echo "ok"
"""
                }
            }

	    }
    }

    post {
        always {
            step([$class: 'WarningsPublisher', parserConfigurations: [[parserName: 'Pep8', pattern: 'results/*.log']]])
        }
    }
}

