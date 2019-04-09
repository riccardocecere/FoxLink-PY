# FoxLink-PY
per lanciare l'applicazione bisogna prima far partire docker desktop

poi par partire la network isolata di comunicazione contenente kafka e zookeeper tramite

    run_kafka_network.sh
    
poi lanciare l'applicazione con

    run_application.sh
    
per stoppare l'applicazione:

    stop_application.sh
    
per stoppare la network isolata di comunicazione kafka:

    stop_kafka_network.sh
    
per cancellare i container dell'applicazione:

    down_application.sh
    
per cancellare i container della network isolata di comunicazione kafka:

    down_kafka_network.sh
