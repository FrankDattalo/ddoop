package ddoop.raft.rpc.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import ddoop.raft.rpc.message.AppendEntitiesResultMessage;
import ddoop.raft.rpc.message.ClientCommandMessage;
import ddoop.raft.rpc.message.MessageType;
import ddoop.raft.rpc.message.RequestVoteResultMessage;
import ddoop.raft.rpc.message.base.BindableAppendEntitiesMessage;
import ddoop.raft.rpc.message.base.BindableClientCommandMessage;
import ddoop.raft.rpc.message.base.BindableRequestVoteMessage;
import ddoop.raft.rpc.message.base.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import ddoop.raft.rpc.Serialization;


/**
 * Implementation of the serialization interface using json.
 */
public class JsonSerialization implements Serialization {

    private static final Logger logger = LoggerFactory.getLogger(JsonSerialization.class);

    /**
     * serializes the message to json prepending a type tag.
     */
    @Override
    public boolean serialize(Message message, OutputStream outputStream) {

        try {
            logger.trace("serialize(message: {})", message);

            byte type = (byte) message.getType().ordinal();
            outputStream.write(type);

            Gson gson = new Gson();
            JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            gson.toJson(message, message.getClass(), jsonWriter);
            jsonWriter.flush();

            return true;

        } catch (IOException e) {
            logger.error("Error serializing message", e);
            return false;
        }
    }

    /**
     * de-serializes the message from json expecting a pre-pended type tag.
     */
    @Override
    public Message deSerialize(InputStream message) {
        
        try {
            InputStreamReader reader = new InputStreamReader(message, StandardCharsets.UTF_8);
            byte type = (byte) reader.read();
            MessageType messageType = MessageType.values()[type];

            Gson gson = new Gson();

            try (JsonReader jsonReader = new JsonReader(reader)) {

                switch (messageType) {
                    case AppendEntities: {
                        return gson.fromJson(jsonReader, BindableAppendEntitiesMessage.class);
                    }
                    case AppendEntitiesResult: {
                        return gson.fromJson(jsonReader, AppendEntitiesResultMessage.class);
                    }
                    case RequestVote: {
                        return gson.fromJson(jsonReader, BindableRequestVoteMessage.class);
                    }
                    case RequestVoteResult:  {
                        return gson.fromJson(jsonReader, RequestVoteResultMessage.class);
                    }
                    case ClientCommand: {
                        return gson.fromJson(jsonReader, BindableClientCommandMessage.class);
                    }
                    case ClientCommandResult: {
                        return gson.fromJson(jsonReader, ClientCommandMessage.class);
                    }
                    default: {
                        logger.error("Unknown message type: {}", messageType);
                        return null;
                    }
                }
            }

        } catch (RuntimeException | IOException e) {
            logger.error("error deserializing message", e);
            return null;
        }
    }
}