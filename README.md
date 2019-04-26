# FoxLink-PY
per lanciare l'applicazione bisogna prima far partire l'agente docker

poi par partire la network isolata di comunicazione contenente mongodb, searx, kafka e zookeeper tramite

    start_environment.sh
    
poi lanciare l'applicazione con

    start_application.sh
    
per stoppare l'applicazione:

    stop_application.sh
    
per stoppare la network isolata di comunicazione:

    stop_environment_network.sh
    
per cancellare i container dell'applicazione:

    delete_application.sh
    
per cancellare i container della network isolata di comunicazione:

    delete_environment.sh
