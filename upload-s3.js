        const uploadToS3 = async (file) => {
            const progress = (e) => {
                console.log(e);
                if (e.currentTarget && e.currentTarget.readyState == 4 && e.currentTarget.status != 200) {
                    try {
                        const parser = new DOMParser();
                        const response = parser.parseFromString(e.currentTarget.responseText, "text/xml");
                        console.log(response)
                        const code = response.getElementsByTagName("Code")[0].textContent
                        const message = response.getElementsByTagName("Message")[0].textContent
                        setMessage(`${message}`)
                        console.log(response.getElementsByTagName("Code")[0].textContent)
                        console.log(response.getElementsByTagName("Message")[0].textContent)
                    } catch (error) {
                        console.log(e.currentTarget.responseText)
                    }

                }
                if (e.lengthComputable) {
                    const percentage =
                        Math.round((e.loaded / e.total) * 90) + 10;
                    console.log(percentage);
                }
                if (e.type == "load") {
                    console.log("Upload Complete")
                }

            }
            const response = await fetch(
                "https://mpobs6jh07.execute-api.us-east-1.amazonaws.com/test/presigned-url-upload",
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "Authorization": store.state.authModule.idToken,
                    },
                    body: JSON.stringify({
                        "bucket_name": "s3-upload-vue-62",
                        "file_name": file.name
                    })
                });

            if (response.ok) {
                console.log("Start Upload")
                const { upload_url } = await response.json();
                console.log(upload_url);
                const xhr = new XMLHttpRequest();
                xhr.upload.addEventListener("progress", progress, false);
                xhr.addEventListener("load", progress, false);
                xhr.addEventListener("error", progress, false);
                xhr.addEventListener("abort", progress, false);
                xhr.open("PUT", upload_url);
                xhr.setRequestHeader("Content-Type", file.type);
                xhr.send(file);
            } else {
                console.log(response)
                try {
                    const errorResponse = await response.json();
                    console.log(errorResponse)
                } catch (e) {
                    setMessage(response.statusText)
                }

            }
        };

        const handleFileUpload = async () => {
            console.log("selected file", file.value.files);
            uploadToS3(file.value.files[0]);
        };
