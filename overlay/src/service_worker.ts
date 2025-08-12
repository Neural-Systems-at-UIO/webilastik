import { Url } from "./util/parsed_url";


interface FetchEvent{
    ///...
    request: Request,
    respondWith: (response: Promise<Response>) => void,
}

type FetchEventListener = (event: FetchEvent) => void;
const addFetchListener = (self.addEventListener as (eventName: "fetch", listener: FetchEventListener) => void)

console.log("Registering service worker!!!")

// The activate handler takes care of cleaning up old caches.
self.addEventListener('activate', _ => {
    console.log("Claiming clients or whatever......");
    (self as any).clients.claim()
});


addFetchListener('fetch', async (event: FetchEvent) => {
    const ebrainsToken = "FIXME: grab token"
    const request = event.request;
    const url = Url.parse(request.url)
    if(
        ebrainsToken === undefined || //FIXME: we should probably prompt the user to log in?
        url.hostname !== "data-proxy.ebrains.eu" ||
        !url.path.raw.startsWith("/api/")
    ){
        event.respondWith(fetch(request))
        return
    }

    let newHeaders = new Headers(request.headers);
    newHeaders.append("Authorization", `Bearer ${ebrainsToken}`);
    let newRequest = new Request(request.url, {
        method: request.method,
        headers: newHeaders,
        body: request.body,
    })
    event.respondWith((async () => {
        const response = await fetch(newRequest)
        if(url.path.name == "stat" || !response.ok){
            return response
        }
        
        // Handle HEAD requests - they don't have a body to parse
        if(request.method.toLowerCase() === "head"){
            // For HEAD requests, we need to get the CSCS URL but make a HEAD request to it
            // However, CSCS doesn't support HEAD, so we make a GET request and strip the body
            try {
                const response_payload = await response.json();
                const cscsObjectUrl = response_payload["url"];
                
                // Make a GET request to CSCS URL but return only headers (simulating HEAD)
                const cscsResponse = await fetch(cscsObjectUrl, {method: "GET"});
                
                // Return response with same headers but no body (HEAD semantics)
                return new Response(null, {
                    status: cscsResponse.status,
                    statusText: cscsResponse.statusText,
                    headers: cscsResponse.headers
                });
            } catch (e) {
                // If we can't parse the response or fetch CSCS URL, return the original response
                return response;
            }
        }
        
        // Only handle GET requests with the redirect logic
        if(request.method.toLowerCase() !== "get"){
            return response
        }
        
        const response_payload = await response.json();
        const cscsObjectUrl = response_payload["url"]
        return await fetch(cscsObjectUrl, {method: "GET"})
    })());
});