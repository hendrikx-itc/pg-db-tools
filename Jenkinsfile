node ('git') {
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
pycodestyle src > pycodestyle.log || echo "ok"
"""
            }

            step([$class: 'JUnitResultArchiver', testResults: '*.xml', healthScaleFactor: 1.0])
        }

        stage ('static-checks') {
            step([$class: 'WarningsPublisher', parserConfigurations: [[parserName: 'Pep8', pattern: 'pycodestyle.log']]])
        }
    }
}

