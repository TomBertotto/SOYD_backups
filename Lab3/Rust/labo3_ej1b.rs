use std::env;
use std::thread;

fn calcular_sum(n: usize) {
    let mut sum: usize = 0;
    for i in 0..n {
        sum = sum + i;
    }
    println!("Sumatoria de {} a {}, resultado: {}", 0, (n-1), sum);
}


fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("Error en argumentos por linea de comando");
        std::process::exit(1);
    }

    let n: usize = args[1].parse().expect("Entero");
    let mut vector_hilos = Vec::with_capacity(n);

    for i in 0..n {
        let tope_sum = i + n;
        let hilo = thread::spawn(move || {
            calcular_sum(tope_sum);
        });
        vector_hilos.push(hilo);
    }   

    for hilo in vector_hilos {
        hilo.join().unwrap();
    }
}
