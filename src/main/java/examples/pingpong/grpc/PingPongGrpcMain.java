package examples.pingpong.grpc;

import java.util.Optional;
import java.util.UUID;

import com.daml.ledger.api.v1.CommandSubmissionServiceGrpc;
import com.daml.ledger.api.v1.CommandSubmissionServiceGrpc.CommandSubmissionServiceFutureStub;
import com.daml.ledger.api.v1.CommandSubmissionServiceOuterClass.SubmitRequest;
import com.daml.ledger.api.v1.CommandsOuterClass.Command;
import com.daml.ledger.api.v1.CommandsOuterClass.Commands;
import com.daml.ledger.api.v1.CommandsOuterClass.CreateCommand;
import com.daml.ledger.api.v1.LedgerIdentityServiceGrpc;
import com.daml.ledger.api.v1.LedgerIdentityServiceGrpc.LedgerIdentityServiceBlockingStub;
import com.daml.ledger.api.v1.LedgerIdentityServiceOuterClass.GetLedgerIdentityRequest;
import com.daml.ledger.api.v1.LedgerIdentityServiceOuterClass.GetLedgerIdentityResponse;
import com.daml.ledger.api.v1.ValueOuterClass.Identifier;
import com.daml.ledger.api.v1.ValueOuterClass.Record;
import com.daml.ledger.api.v1.ValueOuterClass.RecordField;
import com.daml.ledger.api.v1.ValueOuterClass.Value;
import com.daml.ledger.api.v1.admin.UserManagementServiceGrpc;
import com.daml.ledger.api.v1.admin.UserManagementServiceGrpc.UserManagementServiceBlockingStub;
import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass.GetUserRequest;
import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass.GetUserResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class PingPongGrpcMain {

    public static final String APP_ID = "PingPongApp";

    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: HOST PORT [NUM_INITIAL_CONTRACTS]");
            System.exit(-1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        int numInitialContracts = args.length == 3 ? Integer.parseInt(args[2]) : 10;

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

        String ledgerId = fetchLedgerId(channel);

        String aliceParty = fetchPartyId(channel, ALICE_USER);
        String bobParty = fetchPartyId(channel, BOB_USER);

        String packageId = Optional.ofNullable(System.getProperty("package.id"))
                .orElseThrow(() -> new RuntimeException("package.id must be specified via sys properties"));

        Identifier pingIdentifier = Identifier.newBuilder()
                .setPackageId(packageId)
                .setModuleName("PingPong")
                .setEntityName("Ping")
                .build();
        Identifier pongIdentifier = Identifier.newBuilder()
                .setPackageId(packageId)
                .setModuleName("PingPong")
                .setEntityName("Pong")
                .build();

        PingPongProcessor aliceProcessor = new PingPongProcessor(aliceParty, ledgerId, channel, pingIdentifier, pongIdentifier);
        PingPongProcessor bobProcessor = new PingPongProcessor(bobParty, ledgerId, channel, pingIdentifier, pongIdentifier);

        aliceProcessor.runIndefinitely();
        bobProcessor.runIndefinitely();

        createInitialContracts(channel, ledgerId, aliceParty, bobParty, pingIdentifier, numInitialContracts);
        createInitialContracts(channel, ledgerId, bobParty, aliceParty, pingIdentifier, numInitialContracts);


        try {
            Thread.sleep(15000);
            System.exit(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void createInitialContracts(ManagedChannel channel, String ledgerId, String sender, String receiver, Identifier pingIdentifier, int numContracts) {
        CommandSubmissionServiceFutureStub submissionService = CommandSubmissionServiceGrpc.newFutureStub(channel);

        for (int i = 0; i < numContracts; i++) {
            Command createCommand = Command.newBuilder().setCreate(
                    CreateCommand.newBuilder()
                            .setTemplateId(pingIdentifier)
                            .setCreateArguments(
                                    Record.newBuilder()
                                            .setRecordId(pingIdentifier)
                                            .addFields(RecordField.newBuilder().setLabel("sender").setValue(Value.newBuilder().setParty(sender)))
                                            .addFields(RecordField.newBuilder().setLabel("receiver").setValue(Value.newBuilder().setParty(receiver)))
                                            .addFields(RecordField.newBuilder().setLabel("count").setValue(Value.newBuilder().setInt64(0)))
                            )
            ).build();


            SubmitRequest submitRequest = SubmitRequest.newBuilder().setCommands(Commands.newBuilder()
                    .setLedgerId(ledgerId)
                    .setCommandId(UUID.randomUUID().toString())
                    .setWorkflowId(String.format("Ping-%s-%d", sender, i))
                    .setParty(sender)
                    .setApplicationId(APP_ID)
                    .addCommands(createCommand)
            ).build();

            submissionService.submit(submitRequest);
        }
    }

    private static String fetchLedgerId(ManagedChannel channel) {
        LedgerIdentityServiceBlockingStub ledgerIdService = LedgerIdentityServiceGrpc.newBlockingStub(channel);
        GetLedgerIdentityResponse identityResponse = ledgerIdService.getLedgerIdentity(GetLedgerIdentityRequest.getDefaultInstance());
        return identityResponse.getLedgerId();
    }

    private static String fetchPartyId(ManagedChannel channel, String userId) {
        UserManagementServiceBlockingStub userManagementService = UserManagementServiceGrpc.newBlockingStub(channel);
        GetUserResponse getUserResponse = userManagementService.getUser(GetUserRequest.newBuilder().setUserId(userId).build());
        return getUserResponse.getUser().getPrimaryParty();
    }
}