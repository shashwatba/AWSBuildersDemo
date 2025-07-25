<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chatbot UI</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            display: flex;
            flex-direction: column;
            height: 100vh;
        }
        #chat-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 10px;
            overflow-y: auto;
            border: 1px solid #ccc;
        }
        .message {
            margin: 5px 0;
            padding: 10px;
            border-radius: 5px;
            max-width: 70%;
        }
        .user-message {
            align-self: flex-end;
            background-color: #d1e7dd;
        }
        .bot-message {
            align-self: flex-start;
            background-color: #f8d7da;
        }
        #input-container {
            display: flex;
            padding: 10px;
            border-top: 1px solid #ccc;
        }
        #input-container input {
            flex: 1;
            padding: 10px;
            font-size: 16px;
            border: 1px solid #ccc;
            border-radius: 5px;
        }
        #input-container button {
            margin-left: 10px;
            padding: 10px 20px;
            font-size: 16px;
            border: none;
            border-radius: 5px;
            background-color: #007bff;
            color: white;
            cursor: pointer;
        }
        #input-container button:hover {
            background-color: #0056b3;
        }
    </style>
</head>
<body>
    <div id="chat-container"></div>
    <div id="input-container">
        <input type="text" id="user-input" placeholder="Type your message here...">
        <button onclick="sendMessage()">Send</button>
    </div>

    <script>
        const chatContainer = document.getElementById('chat-container');

        function sendMessage() {
            const userInput = document.getElementById('user-input');
            const message = userInput.value.trim();
            if (message === '') return;

            // Add user message to chat
            addMessageToChat(message, 'user-message');

            // Simulate bot response
            setTimeout(() => {
                const botResponse = getBotResponse(message);
                addMessageToChat(botResponse, 'bot-message');
            }, 500);

            userInput.value = '';
        }

        function addMessageToChat(message, className) {
            const messageElement = document.createElement('div');
            messageElement.className = `message ${className}`;
            messageElement.textContent = message;
            chatContainer.appendChild(messageElement);
            chatContainer.scrollTop = chatContainer.scrollHeight;
        }

        function getBotResponse(userMessage) {
            // Simple bot logic
            if (userMessage.toLowerCase().includes('hello')) {
                return 'Hi there! How can I help you?';
            } else if (userMessage.toLowerCase().includes('bye')) {
                return 'Goodbye! Have a great day!';
            } else {
                return 'I am just a simple bot. Can you rephrase that?';
            }
        }
    </script>
</body>
</html>
