node ('git') {
    stage ('checkout') {
        checkout scm
    }

    stage ('build') {
        def img = docker.build 'pg-db-tools:snapshot'

        stage ('test') {
            img.inside ("-v /etc/passwd:/etc/passwd -v /etc/group:/etc/group") {
                sh 'pip3 install .'
                sh 'pip3 install unittest-xml-reporting pep8'
                /* Run unit tests */
                sh 'python3 -m xmlrunner discover --start-directory tests'

                /* Run static code analysis */
                sh 'pep8 . > pep8.log || echo "ok"'
            }

            step([$class: 'JUnitResultArchiver', testResults: '*.xml', healthScaleFactor: 1.0])
        }

        stage ('static-checks') {
            step([$class: 'WarningsPublisher', parserConfigurations: [[parserName: 'Pep8', pattern: 'pep8.log']]])
        }
    }
}

