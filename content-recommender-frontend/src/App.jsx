import React from 'react';
import ContentRecommender from "./components/ContentRecommender";

function App() {
    return (
        <div className="min-h-screen bg-background text-foreground">
            <header className="border-b border-border/20 bg-card/30 backdrop-blur supports-[backdrop-filter]:bg-card/30">
                <div className="container flex h-16 items-center">
                    <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
                        Media Scout
                    </h1>
                </div>
            </header>

            <main className="container py-8 md:py-12">
                <div className="mx-auto max-w-2xl">
                    <h2 className="mb-2 text-3xl font-bold tracking-tight">
                        Welcome to Media Scout
                    </h2>
                    <p className="mb-8 text-muted-foreground">
                        Get personalized movie and TV show recommendations based on your preferences.
                    </p>
                    <ContentRecommender />
                </div>
            </main>

            <footer className="border-t border-border/20 bg-card/30 backdrop-blur supports-[backdrop-filter]:bg-card/30">
                <div className="container flex h-14 items-center justify-center text-sm text-muted-foreground">
                    © 2024 Media Scout. Powered by TMDB.
                </div>
            </footer>
        </div>
    );
}

export default App;