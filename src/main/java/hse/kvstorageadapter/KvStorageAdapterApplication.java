package hse.kvstorageadapter;

import hse.kvstorageadapter.service.SenderService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class KvStorageAdapterApplication {

	public static void main(String[] args) {
		ApplicationContext context = SpringApplication.run(KvStorageAdapterApplication.class, args);
		
		SenderService senderService = context.getBean(SenderService.class);
		
		for (int i = 0; i < 10; i++) {
			senderService.sendMessage(String.valueOf(i), String.valueOf(i));
		}
		
		for (int i = 0; i < 10; i++) {
			String message = senderService.getByKey(String.valueOf(i));
			System.out.println(message);
		}
		
	}

}
