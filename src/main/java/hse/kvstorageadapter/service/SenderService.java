package hse.kvstorageadapter.service;

import hse.kvstorageadapter.grpc.DeleteIn;
import hse.kvstorageadapter.grpc.GetIn;
import hse.kvstorageadapter.grpc.GetOut;

import hse.kvstorageadapter.grpc.KVStoreGrpc;
import hse.kvstorageadapter.grpc.PutIn;
import hse.kvstorageadapter.grpc.PutOut;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import jakarta.annotation.PostConstruct;
import net.devh.boot.grpc.client.inject.GrpcClient;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;


@Service
public class SenderService {
	
	@Value("${kvstore.username}")
	private String username;
	
	@Value("${kvstore.password}")
	private String password;
	
	@GrpcClient("SenderService")
	private KVStoreGrpc.KVStoreBlockingStub stub;
	
	private final int[] probablePortsOfMaser = {8090, 8091, 8092};
	
	private int lastIndexOfMaster = 0;
	
	@PostConstruct
	public void init() {
		setMetadata();
		findMasterPort(lastIndexOfMaster);
	}
	
	public String sendMessage(String key, String value) {
		PutIn putIn = PutIn.newBuilder()
							.setKey(key)
							.setValue(value)
							.setTtl(0)
							.build();
		
		PutOut reply = stub.put(putIn);
		
		return reply.toString();
	}
	
	public String getByKey(String key) {
		GetIn getIn = GetIn.newBuilder()
							.setKey(key)
							.build();
		
		GetOut reply = stub.get(getIn);
		
		return reply.toString();
	}
	
	public void deleteByKey(String key) {
		DeleteIn deleteIn = DeleteIn.newBuilder()
									.setKey(key)
									.build();
		stub.delete(deleteIn);
	}
	
	public String consistentGet(String key) {
		GetIn getIn = GetIn.newBuilder()
							.setKey(key)
							.build();
		
		GetOut reply = stub.get(getIn);
		return reply.toString();
	}
	
	private void findMasterPort(int index) {
		try {
			sendMessage("unexistedKey", "unexistedValue");
			lastIndexOfMaster = index;
		} catch (io.grpc.StatusRuntimeException e) {
			findMasterPort((index + 1) % probablePortsOfMaser.length);
		}
		
		deleteByKey("unexistedKey");
	}
	
	private void setMetadata() {
		ManagedChannel channel = ManagedChannelBuilder
				.forAddress("localhost", probablePortsOfMaser[lastIndexOfMaster])
				.usePlaintext()
				.build();
		
		Metadata metadata = new Metadata();
		Metadata.Key<String> usernameKey = Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER);
		Metadata.Key<String> passwordKey = Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER);
		metadata.put(usernameKey, username);
		metadata.put(passwordKey, password);
		
		ClientInterceptor interceptor = MetadataUtils.newAttachHeadersInterceptor(metadata);
		
		this.stub = KVStoreGrpc.newBlockingStub(channel).withInterceptors(interceptor);
	}
}