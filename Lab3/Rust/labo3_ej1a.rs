use std::env;
use std::thread;
type PthreadT = std::os::raw::c_ulong;


extern "C" {
    fn pthread_self() -> PthreadT;
}

fn mostrar_id() {
    unsafe {
        let tid = pthread_self();
        println!("Hilo con ID: {:?}", tid);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("Error en argumentos por linea de comando");
        std::process::exit(1);
    }
    
    let n: usize = args[1].parse().expect("Entero");
    let mut handles = Vec::with_capacity(n);

    for _ in 0..n {
        let handle = thread::spawn(|| {
            mostrar_id();
        });
        handles.push(handle);
    }


    for handle in handles {
        handle.join().unwrap();
    }
}
